import json
import traceback
from pyspark.sql import DataFrame as sdf, SparkSession

# Since 11.x, databricks.koalas has been moved to pyspark.pandas
try:
    from pyspark.pandas import DataFrame as kdf
except ImportError:
    from databricks.koalas import DataFrame as kdf

class FlowFunction(object):
    """
    This is the Python implementation of the Scala trait PythonFlowFunction. This wraps
    the user-defined flow function such that it can be called from the JVM during graph init.
    """

    def __init__(self, func):
        """
        func should be a function that takes no parameters and returns a Python or Koalas DataFrame
        """
        self.func = func

    def call(self):
        """
        This function is called by Py4J and returns the underlying Java DataFrame returned
        by func back to the JVM
        """
        res = self.func()
        if isinstance(res, kdf):
            df = res.to_spark()
        elif isinstance(res, sdf):
            df = res
        else:
            function_name = self.func.__name__
            return_type = type(res).__name__
            raise RuntimeError(
                f"Query defined in function '{function_name}' returned '{return_type}'. "
                f"It must return either a Spark or Koalas DataFrame.")
        return df._jdf

    class Java:
        implements = ['com.databricks.pipelines.QueryFunction']

class _PythonDebugger(object):
    """
    This is the Python implementation of the Scala trait PythonDebugger. This allows us calling Python debugging code
    from JVM such as generating a Python thread dump.
    """

    def __init__(self, notebook_path):
        self._notebook_path = notebook_path

    def notebookPath(self):
        return self._notebook_path

    def fetchThreadDump(self):
        import sys
        import traceback

        thread_dump = dict((tid, "".join(traceback.format_stack(frame))) for tid, frame in sys._current_frames().items())
        return json.dumps(thread_dump)

    class Java:
        implements = ['com.databricks.pipelines.execution.core.debugger.PythonDebugger']

class EventHook(object):
    """
    This is the python implementation of the Scala trait EventHook. This provides us to access user defined python hooks from the JVM, so we can register, execute, and apply error handling logic on them.
    """
    def __init__(self, func, max_allowable_consecutive_failures):
        """
        EventHook initialization.
        :param func: the user defined python hook. Should be a function that accepts a single parameter (a JSON event string).
        """
        if not callable(func):
            raise "Event hooks must be single argument function types."
        self._func = func
        self._is_fatal = False
        self._max_allowable_consecutive_failures = max_allowable_consecutive_failures
        self._spark = SparkSession.builder.getOrCreate()

    def isFatal(self):
        """
        Returns whether this event hook's failures are considered fatal or not
        :return: Boolean representing whether this event hook is fatal
        """
        return self._is_fatal

    def maxAllowableConsecutiveFailures(self):
        """
        Returns the maximum number of consecutive execution failures allowed for this hook
        :return: Integer representing the maximum number of consecutive execution failures allowed for this hook
        """
        return self._max_allowable_consecutive_failures

    def name(self):
        """
        Returns the name associated with this hook.
        :return: The name of the hook, which is taken to be the user's function name. Uniqueness is enforced.
        """
        return self._func.__name__

    def call(self, event):
        """
        Executes the stored user defined hook.
        :param event: the JSON string representation of an event, passed onto the user function as JSON.
        """

        # Since any potential exceptions raised by the user's event hook function are not propagated back to the backend, we wrap the execution in a try/catch and return an execution status object representing the outcome.
        try:
            self._func(json.loads(event))
            return self._spark._jvm.com.databricks.pipelines.EventHookSuccess.apply()
        except Exception as e:
            return self._spark._jvm.com.databricks.pipelines.EventHookFailure.apply(
                str(e),
                traceback.format_exc()
            )

    class Java:
        implements = ['com.databricks.pipelines.EventHook']
