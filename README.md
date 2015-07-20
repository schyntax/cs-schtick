# C# Schtick

[![NuGet version](https://badge.fury.io/nu/Schyntax.svg)](http://badge.fury.io/nu/Schtick)
[![Build status](https://ci.appveyor.com/api/projects/status/9y3f0tnvtmiyyfw7/branch/master?svg=true)](https://ci.appveyor.com/project/bretcope/cs-schtick/branch/master)

A scheduled task runner built on [Schyntax](https://github.com/schyntax/schyntax) (a domain-specific language for defining event schedules in a terse, but readable, format).

## Usage

> Best practice is to make a singleton instance of the `Schtick` class.

```csharp
using Schyntax;

var schtick = new Schtick();

// setup an exception handler so we know when tasks blow up
schtick.OnTaskException += (task, exception) => LogException(ex);

// add a task which will call DoSomeTask every hour at 15 minutes past the hour
schtick.AddTask("unique-task-name", "min(15)", (task, timeIntendedToRun) => DoSomeTask());
```

> For complete documentation of schedule format language itself, see the [Schyntax](https://github.com/schyntax/schyntax) project.

`AddTask` has several optional arguments which are documented in [Schtick.cs](https://github.com/schyntax/cs-schtick/blob/master/Schtick/Schtick.cs) and will show up in intellisense.

### Async Task Callbacks

For async callbacks, use `Schtick.AddAsyncTask()`.

```csharp
schtick.AddAsyncTask("task-name", "hour(*)", async (task, time) => await DoSomethingAsync());
```

### ScheduledTask Objects

`AddTask` and `AddAsyncTask` return an instance of `ScheduledTask`. This object has properties like `Name`, `IsScheduleRunning`, and `IsCallbackExecuting`. It can be used to start and stop the schedule via `StartSchedule()` and `StopSchedule`. There is also an `UpdateSchedule` method which allows you keep the same task, but run it on a different schedule.

The `ScheduledTask` object is also the first argument to all task callbacks.

### Application Shutdown

To ensure the application doesn't terminate in the middle of an executing callback, you should call the `Schtick.Shutdown()` method before the app terminates. `Shutdown` is an async method which completes when all scheduled tasks have been stopped and all running callbacks have completed.

A good way to do this in an ASP.NET app is to use a registered object.

```csharp
public class RegisteredSchtick : IRegisteredObject
{
	public Schtick Schtick { get; }
	
	public RegisteredSchtick(Schtick schtick)
	{
		Schtick = schtick;
	}

	public void Stop(bool immediate)
	{
		if (!Schtick.IsShuttingDown)
		{
			Schtick.Shutdown().ContinueWith(task => HostingEnvironment.UnregisterObject(this));
		}
	}
}
```

On app start:

```csharp
_schtick = new Schtick();
HostingEnvironment.RegisterObject(new RegisteredSchtick(_schtick));
```
