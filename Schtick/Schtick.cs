using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Schyntax
{
    public delegate void ScheduledTaskCallback(ScheduledTask task, DateTimeOffset timeIntendedToRun);
    public delegate Task ScheduledTaskAsyncCallback(ScheduledTask task, DateTimeOffset timeIntendedToRun);

    public class Schtick
    {
        readonly object _lockTasks = new object();
        readonly Dictionary<string, ScheduledTask> _tasks = new Dictionary<string, ScheduledTask>();
        readonly object _lockHeap = new object();
        readonly PendingEventHeap _eventHeap = new PendingEventHeap();

        public bool IsShuttingDown { get; private set; }

        public event Action<ScheduledTask, Exception> OnTaskException;

        public TaskCollection Tasks => new TaskCollection(_tasks.Values);

        public Schtick()
        {
            Task.Run((Func<Task>)Poll);
        }

        /// <summary>
        /// Adds a scheduled task to this instance of Schtick.
        /// </summary>
        /// <param name="name">A unique name for this task. If null, a guid will be used.</param>
        /// <param name="schedule">A Schyntax schedule string.</param>
        /// <param name="callback">Function which will be called each time the task is supposed to run.</param>
        /// <param name="autoRun">If true, Start() will be called on the task automatically.</param>
        /// <param name="lastKnownEvent">The last Date when the task is known to have run. Used for Task Windows.</param>
        /// <param name="window">
        /// The period of time after an event should have run where it would still be appropriate to run it.
        /// See Task Windows documentation for more details.
        /// </param>
        public ScheduledTask AddTask(
            string name,
            string schedule,
            ScheduledTaskCallback callback,
            bool autoRun = true,
            DateTimeOffset lastKnownEvent = default(DateTimeOffset),
            TimeSpan window = default(TimeSpan))
        {
            if (schedule == null)
                throw new ArgumentNullException(nameof(schedule));

            if (callback == null)
                throw new ArgumentNullException(nameof(callback));

            return AddTaskImpl(name, new Schedule(schedule), callback, null, autoRun, lastKnownEvent, window);
        }

        /// <summary>
        /// Adds a scheduled task to this instance of Schtick.
        /// </summary>
        /// <param name="name">A unique name for this task. If null, a guid will be used.</param>
        /// <param name="schedule">A Schyntax schedule string.</param>
        /// <param name="asyncCallback">Function which will be called each time the task is supposed to run.</param>
        /// <param name="autoRun">If true, Start() will be called on the task automatically.</param>
        /// <param name="lastKnownEvent">The last Date when the task is known to have run. Used for Task Windows.</param>
        /// <param name="window">
        /// The period of time after an event should have run where it would still be appropriate to run it.
        /// See Task Windows documentation for more details.
        /// </param>
        public ScheduledTask AddAsyncTask(
            string name,
            string schedule,
            ScheduledTaskAsyncCallback asyncCallback,
            bool autoRun = true,
            DateTimeOffset lastKnownEvent = default(DateTimeOffset),
            TimeSpan window = default(TimeSpan))
        {
            if (schedule == null)
                throw new ArgumentNullException(nameof(schedule));

            if (asyncCallback == null)
                throw new ArgumentNullException(nameof(asyncCallback));

            return AddTaskImpl(name, new Schedule(schedule), null, asyncCallback, autoRun, lastKnownEvent, window);
        }

        /// <summary>
        /// Adds a scheduled task to this instance of Schtick.
        /// </summary>
        /// <param name="name">A unique name for this task. If null, a guid will be used.</param>
        /// <param name="schedule">A Schyntax Schedule object.</param>
        /// <param name="callback">Function which will be called each time the task is supposed to run.</param>
        /// <param name="autoRun">If true, Start() will be called on the task automatically.</param>
        /// <param name="lastKnownEvent">The last Date when the task is known to have run. Used for Task Windows.</param>
        /// <param name="window">
        /// The period of time after an event should have run where it would still be appropriate to run it.
        /// See Task Windows documentation for more details.
        /// </param>
        public ScheduledTask AddTask(
            string name,
            Schedule schedule,
            ScheduledTaskCallback callback,
            bool autoRun = true,
            DateTimeOffset lastKnownEvent = default(DateTimeOffset),
            TimeSpan window = default(TimeSpan))
        {
            if (callback == null)
                throw new ArgumentNullException(nameof(callback));

            return AddTaskImpl(name, schedule, callback, null, autoRun, lastKnownEvent, window);
        }


        /// <summary>
        /// Adds a scheduled task to this instance of Schtick.
        /// </summary>
        /// <param name="name">A unique name for this task. If null, a guid will be used.</param>
        /// <param name="schedule">A Schyntax Schedule object.</param>
        /// <param name="asyncCallback">Function which will be called each time the task is supposed to run.</param>
        /// <param name="autoRun">If true, Start() will be called on the task automatically.</param>
        /// <param name="lastKnownEvent">The last Date when the task is known to have run. Used for Task Windows.</param>
        /// <param name="window">
        /// The period of time after an event should have run where it would still be appropriate to run it.
        /// See Task Windows documentation for more details.
        /// </param>
        public ScheduledTask AddAsyncTask(
            string name,
            Schedule schedule,
            ScheduledTaskAsyncCallback asyncCallback,
            bool autoRun = true,
            DateTimeOffset lastKnownEvent = default(DateTimeOffset),
            TimeSpan window = default(TimeSpan))
        {
            if (asyncCallback == null)
                throw new ArgumentNullException(nameof(asyncCallback));

            return AddTaskImpl(name, schedule, null, asyncCallback, autoRun, lastKnownEvent, window);
        }

        ScheduledTask AddTaskImpl(
            string name,
            Schedule schedule,
            ScheduledTaskCallback callback,
            ScheduledTaskAsyncCallback asyncCallback,
            bool autoRun,
            DateTimeOffset lastKnownEvent,
            TimeSpan window)
        {
            if (schedule == null)
                throw new ArgumentNullException(nameof(schedule));

            if (name == null)
                name = Guid.NewGuid().ToString();
            
            ScheduledTask task;
            lock (_lockTasks)
            {
                if (IsShuttingDown)
                    throw new Exception("Cannot add a task to Schtick after Shutdown() has been called.");

                if (_tasks.ContainsKey(name))
                    throw new Exception($"A scheduled task named \"{name}\" already exists.");

                task = new ScheduledTask(this, name, schedule, callback, asyncCallback)
                {
                    Window = window,
                    IsAttached = true,
                };

                _tasks.Add(name, task);
            }

            task.OnException += TaskOnOnException;

            if (autoRun)
                task.StartSchedule(lastKnownEvent);

            return task;
        }

        void TaskOnOnException(ScheduledTask task, Exception ex)
        {
            var ev = OnTaskException;
            ev?.Invoke(task, ex);
        }

        public bool TryGetTask(string name, out ScheduledTask task)
        {
            return _tasks.TryGetValue(name, out task);
        }

        public ScheduledTask[] GetAllTasks()
        {
            lock (_lockTasks)
            {
                // could be one line of linq, but eh, this is cheaper
                var tasks = new ScheduledTask[_tasks.Count];
                var i = 0;
                foreach(var t in _tasks)
                {
                    tasks[i] = t.Value;
                    i++;
                }

                return tasks;
            }
        }

        public bool RemoveTask(string name)
        {
            lock (_lockTasks)
            {
                if (IsShuttingDown)
                    throw new Exception("Cannot remove a task from Schtick after Shutdown() has been called.");

                ScheduledTask task;
                if (!_tasks.TryGetValue(name, out task))
                    return false;

                if (task.IsScheduleRunning)
                    throw new Exception($"Cannot remove task \"{name}\". It is still running.");

                task.IsAttached = false;
                _tasks.Remove(name);
                return true;
            }
        }

        public async Task Shutdown()
        {
            ScheduledTask[] tasks;
            lock (_lockTasks)
            {
                IsShuttingDown = true;
                tasks = GetAllTasks();
            }

            foreach (var t in tasks)
            {
                t.IsAttached = false; // prevent anyone from calling start on the task again
                t.StopSchedule();
            }

            while (true)
            {
                var allStopped = true;
                foreach (var t in tasks)
                {
                    if (t.IsCallbackExecuting)
                    {
                        allStopped = false;
                        break;
                    }
                }

                if (allStopped)
                    return;

                await Task.Delay(10).ConfigureAwait(false); // wait 10 milliseconds, then check again
            }
        }

        internal void AddPendingEvent(PendingEvent ev)
        {
            if (IsShuttingDown) // don't care about adding anything if we're shutting down
                return;

            lock (_lockHeap)
            {
                _eventHeap.Push(ev);
            }
        }

        async Task Poll()
        {
            // figure out the initial delay
            var now = DateTimeOffset.UtcNow;
            DateTimeOffset intendedTime = new DateTimeOffset(now.Year, now.Month, now.Day, now.Hour, now.Minute, now.Second, 0, now.Offset);
            if (now.Millisecond > 0)
            {
                await Task.Delay(1000 - now.Millisecond);
                intendedTime = intendedTime.AddSeconds(1);
            }

            while (true)
            {
                if (IsShuttingDown)
                    return;

                PopAndRunEvents(intendedTime);

                // figure out the next second to poll on
                now = DateTimeOffset.UtcNow;
                do
                {
                    intendedTime = intendedTime.AddSeconds(1);
                }
                while (intendedTime < now);

                await Task.Delay(intendedTime - now);
            }
        }

        void PopAndRunEvents(DateTimeOffset intendedTime)
        {
            lock (_lockHeap)
            {
                while (_eventHeap.Count > 0 && _eventHeap.Peek().ScheduledTime <= intendedTime)
                {
                    _eventHeap.Pop().Run(); // queues for running on the thread pool
                }
            }
        }

        public struct TaskCollection : IReadOnlyCollection<ScheduledTask>
        {
            readonly Dictionary<string, ScheduledTask>.ValueCollection _values;

            public int Count => _values.Count;

            internal TaskCollection(Dictionary<string, ScheduledTask>.ValueCollection values)
            {
                _values = values;
            }

            public Enumerator GetEnumerator()
            {
                return new Enumerator(_values.GetEnumerator());
            }

            IEnumerator<ScheduledTask> IEnumerable<ScheduledTask>.GetEnumerator() => GetEnumerator();
            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

            public struct Enumerator : IEnumerator<ScheduledTask>
            {
                Dictionary<string, ScheduledTask>.ValueCollection.Enumerator _enumerator;

                public ScheduledTask Current => _enumerator.Current;

                object IEnumerator.Current => _enumerator.Current;

                internal Enumerator(Dictionary<string, ScheduledTask>.ValueCollection.Enumerator enumerator)
                {
                    _enumerator = enumerator;
                }

                public bool MoveNext() => _enumerator.MoveNext();
                public void Dispose() => _enumerator.Dispose();
                void IEnumerator.Reset() => ((IEnumerator)_enumerator).Reset();
            }
        }

    }

    public class ScheduledTask
    {
        readonly object _scheduleLock = new object();
        int _runId = 0;
        int _execLocked = 0;
        readonly Schtick _schtick;

        public string Name { get; }
        public Schedule Schedule { get; private set; }
        public ScheduledTaskCallback Callback { get; }
        public ScheduledTaskAsyncCallback AsyncCallback { get; }
        public bool IsScheduleRunning { get; internal set; }
        public bool IsCallbackExecuting => _execLocked == 1;
        public bool IsAttached { get; internal set; }
        public TimeSpan Window { get; set; }
        public DateTimeOffset NextEvent { get; private set; }
        public DateTimeOffset PrevEvent { get; private set; }

        public event Action<ScheduledTask, Exception> OnException;

        internal ScheduledTask(Schtick schtick, string name, Schedule schedule, ScheduledTaskCallback callback, ScheduledTaskAsyncCallback asyncCallback)
        {
            _schtick = schtick;
            Name = name;
            Schedule = schedule;

            if ((callback == null) == (asyncCallback == null))
                throw new Exception("callback or asyncCallback must be specified, but not both.");

            Callback = callback;
            AsyncCallback = asyncCallback;
        }

        public void StartSchedule(DateTimeOffset lastKnownEvent = default(DateTimeOffset))
        {
            lock (_scheduleLock)
            {
                if (!IsAttached)
                    throw new InvalidOperationException("Cannot start task which is not attached to a Schtick instance.");

                if (IsScheduleRunning)
                    return;

                var firstEvent = default(DateTimeOffset);
                var firstEventSet = false;
                var window = Window;
                if (window > TimeSpan.Zero && lastKnownEvent != default(DateTimeOffset))
                {
                    // check if we actually want to run the first event right away
                    var prev = Schedule.Previous();
                    lastKnownEvent = lastKnownEvent.AddSeconds(1); // add a second for good measure
                    if (prev > lastKnownEvent && prev > (DateTimeOffset.UtcNow - window))
                    {
                        firstEvent = prev;
                        firstEventSet = true;
                    }
                }

                if (!firstEventSet)
                    firstEvent = Schedule.Next();

                while (firstEvent <= PrevEvent)
                {
                    // we don't want to run the same event twice
                    firstEvent = Schedule.Next(firstEvent);
                }

                NextEvent = firstEvent;
                IsScheduleRunning = true;
                QueueNextEvent();
            }
        }
        
        public void StopSchedule()
        {
            lock (_scheduleLock)
            {
                if (!IsScheduleRunning)
                    return;

                _runId++;
                IsScheduleRunning = false;
            }
        }

        public void UpdateSchedule(string schedule)
        {
            if (Schedule.OriginalText == schedule)
                return;

            UpdateSchedule(new Schedule(schedule));
        }

        public void UpdateSchedule(Schedule schedule)
        {
            if (schedule == null)
                throw new ArgumentNullException(nameof(schedule));

            lock (_scheduleLock)
            {
                var wasRunning = IsScheduleRunning;
                if (wasRunning)
                    StopSchedule();

                Schedule = schedule;

                if (wasRunning)
                    StartSchedule();
            }
        }

        internal async Task RunPendingEvent(PendingEvent ev)
        {
            var eventTime = ev.ScheduledTime;
            var execLockTaken = false;
            try
            {
                lock (_scheduleLock)
                {
                    if (ev.RunId != _runId)
                        return;

                    // take execution lock
                    execLockTaken = Interlocked.CompareExchange(ref _execLocked, 1, 0) == 0;
                    if (execLockTaken)
                        PrevEvent = eventTime; // set this here while we're still in the schedule lock
                }

                if (execLockTaken)
                {
                    try
                    {
                        if (Callback != null)
                            Callback(this, eventTime);
                        else
                            await AsyncCallback(this, eventTime).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        RaiseException(ex);
                    }
                }
            }
            finally
            {
                if (execLockTaken)
                    _execLocked = 0; // release exec lock
            }

            // figure out the next time to run the schedule
            lock (_scheduleLock)
            {
                if (ev.RunId != _runId)
                    return;

                try
                {
                    var next = Schedule.Next();
                    if (next <= eventTime)
                        next = Schedule.Next(eventTime);

                    NextEvent = next;
                    QueueNextEvent();
                }
                catch (Exception ex)
                {
                    _runId++;
                    IsScheduleRunning = false;
                    RaiseException(new ScheduleCrashException("Schtick Schedule has been terminated because the next valid time could not be found.", this, ex));
                }
            }
        }

        void QueueNextEvent()
        {
            _schtick.AddPendingEvent(new PendingEvent(NextEvent, this, _runId));
        }

        void RaiseException(Exception ex)
        {
            Task.Run(() =>
            {
                var ev = OnException;
                ev?.Invoke(this, ex);
            });
        }
    }
}
