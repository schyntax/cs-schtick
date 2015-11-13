using System;

namespace Schyntax
{
    internal class PendingEvent
    {
        public DateTimeOffset ScheduledTime { get; }
        public ScheduledTask Task { get; }
        public int RunId { get; }

        internal PendingEvent(DateTimeOffset scheduledTime, ScheduledTask task, int runId)
        {
            ScheduledTime = scheduledTime;
            Task = task;
            RunId = runId;
        }

        internal bool IsEarlierThan(PendingEvent ev)
        {
            return ScheduledTime < ev.ScheduledTime;
        }

        internal void Run()
        {
            System.Threading.Tasks.Task.Run(() => Task.RunPendingEvent(this));
        }
    }

    internal class PendingEventHeap
    {
        private PendingEvent[] _events = new PendingEvent[16];

        public int Count { get; private set; }

        public void Push(PendingEvent ev)
        {
            if (ev == null)
                throw new ArgumentNullException(nameof(ev));

            var ei = Add(ev);
            while (ei > 0)
            {
                var pi = ParentIndex(ei);
                if (ev.IsEarlierThan(_events[pi]))
                {
                    Swap(ei, pi);
                    ei = pi;
                }
                else
                {
                    break;
                }
            }
        }

        public PendingEvent Pop()
        {
            if (Count == 0)
                throw new Exception("Pop called on empty heap.");

            var ret = _events[0];
            var end = ClearEndElement();
            if (Count > 0)
            {
                _events[0] = end;
                var ei = 0;

                while (true)
                {
                    var lci = LeftChildIndex(ei);
                    var rci = RightChildIndex(ei);

                    if (lci < Count && _events[lci].IsEarlierThan(end))
                    {
                        // we know the left child is earlier than the parent, but we have to check if the right child is actually the correct parent
                        if (rci < Count && _events[rci].IsEarlierThan(_events[lci]))
                        {
                            // right child is earlier than left child, so it's the correct parent
                            Swap(ei, rci);
                            ei = rci;
                        }
                        else
                        {
                            // left is the correct parent
                            Swap(ei, lci);
                            ei = lci;
                        }
                    }
                    else if (rci < Count && _events[rci].IsEarlierThan(end))
                    {
                        // only the right child is earlier than the parent, so we know that's the one to swap
                        Swap(ei, rci);
                        ei = rci;
                    }
                    else
                    {
                        break;
                    }
                }
            }

            return ret;
        }

        public PendingEvent Peek()
        {
            return Count > 0 ? _events[0] : null;
        }

        private static int ParentIndex(int index)
        {
            return (index - 1) / 2;
        }

        private static int LeftChildIndex(int index)
        {
            return 2 * (index + 1);
        }

        private static int RightChildIndex(int index)
        {
            return 2 * (index + 1) - 1;
        }

        private void Swap(int a, int b)
        {
            var temp = _events[a];
            _events[a] = _events[b];
            _events[b] = temp;
        }

        // returns the index of the added item
        private int Add(PendingEvent ev)
        {
            // check if we need to resize
            if (_events.Length == Count)
            {
                var bigger = new PendingEvent[_events.Length * 2];
                Array.Copy(_events, bigger, Count);
                _events = bigger;
            }

            _events[Count] = ev;
            return Count++; // postfix is intentional
        }

        private PendingEvent ClearEndElement()
        {
            Count--;
            var ev = _events[Count];
            _events[Count] = null; // so the GC can release it
            return ev;
        }
    }
}