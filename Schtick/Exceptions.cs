using System;

namespace Schyntax
{
    public sealed class ScheduleCrashException : SchyntaxException
    {
        public ScheduledTask Task { get; }

        internal ScheduleCrashException(string message, ScheduledTask task, Exception innerException) : base(message, innerException)
        {
            Task = task;
            Data["TaskName"] = task.Name;
            Data["Schedule"] = task.Schedule.OriginalText;
        }
    }
}
