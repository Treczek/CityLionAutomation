# pylint: disable=C0301

"""
Classes implemented in this module can help to build easier to understand and more readable code by grouping several
methods into sequential steps. Steps Class is a Sequence object that contains single Step objects. Step object holds a
function, and argument for this function, and state parameter - elapsed_time, if step has finished running.

Steps can be easily logged to the logging handler or printed using repr or str built-in methods.
"""

from time import perf_counter
import structlog


def apply_steps(steps, apply_to=None, steps_name=None, logger=None, verbose=True):
    """
    This method is use to steer running of specific modules. The main idea behind using it is to encapsulate smaller
    parts of code in functions that take one parameter, and returned it after changes.
    :param steps: list of tuples with all steps that will be done in the for loop. Tuples contains name of the function,
    and dictionary with parameters to this functions. Parameters are passed to every function using ** operator.
    :param apply_to: If you want to pass some object to the first function, you can use this parameter. Otherwise, it
    will be generated after first step
    :param steps_name: name of the process that will be passed to the logger
    :return: Whatever is returned in the last step function
    """

    logger = logger or structlog.getLogger(__name__)

    steps = steps if isinstance(steps, Steps) else Steps(steps)
    final_output = steps(apply_to)

    # Logging how much time every step take
    if verbose:
        logger.info(f"\n{'#'*25} {steps_name if steps_name else 'Runtimes'} per step in seconds {'#'*25}")
        run_times_dict, total_time = steps.get_run_times()
        for step, runtime in run_times_dict.items():
            logger.info(f"{step} - {runtime}")
        logger.info(f"{'#'*25} Total time: {round(total_time, 2)} {'#'*25}\n")

    return final_output


class Step:
    """
    Single step with function and keyword arguments that will be used if called
    """
    def __init__(self, function, kwargs=None):
        """
        :param function: function object that will be used while calling step
        :param kwargs: dictionary with keyword arguments of this function
        """
        self.function = function
        self.kwargs = kwargs
        self.elapsed_time = "not_runned"

    def __call__(self, obj=None):
        """
        Simply running held function with its parameters on given object, and timing it
        """
        start_time = perf_counter()
        result = self.function(obj, **self.kwargs) if self.kwargs else self.function(obj)
        self.elapsed_time = round(perf_counter() - start_time, 3)
        return result

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return f"{self.function.__name__}" + \
                             (f" with arguments: {self.kwargs}." if self.kwargs else " with no arguments.")

    def __iter__(self):
        return self._step_generator()

    def _step_generator(self):
        """
        Generator allow us to unpack single Step into function, argument using tuple unpacking technique.
        """
        yield self.function
        yield self.kwargs

    def change_kwarg(self, kwarg, new_value):
        """
        Change given Step argument into a new one.
        """

        try:
            self.kwargs[kwarg] = new_value
        except KeyError as exc:
            raise KeyError("Given argument does not exist in step arguments.") from exc


class Steps:
    """
    Sequence based list of Step objects. For more information refer to the module documentation on top of this module.
    """
    def __init__(self, steps):
        self._steps = [Step(func, args) for func, args in steps]

    def __call__(self, pandas_pipe_obj):
        """
        Passing given object through all steps.
        """
        return self.apply_to(pandas_pipe_obj)

    def __len__(self):
        return len(self._steps)

    def __getitem__(self, index_or_name):
        """
        Indexing can be done using sequence index or function name.
        TODO - at the current state, indexing by function name will return the first function with such name in the seq.
        """
        if isinstance(index_or_name, str):
            for target_index, (func, _) in enumerate(self._steps):
                if func.__name__ == index_or_name:
                    index_or_name = target_index
        return self._steps[index_or_name]

    def __str__(self):
        return repr(self)

    def __repr__(self):
        description = "\n".join([f"{step}" for step in self._steps])
        elapsed_time = "\n".join([f"{step.function.__name__} - {step.elapsed_time} sec" for step in self._steps])
        return f"{description}\n\n{elapsed_time}"

    def __iter__(self):
        return iter(self._steps)

    def append(self, function=None, argument_dict=None, step=None):
        """
        Creation of additional step at the end of the steps sequence. You can use Step object or pass function and
        argument dict.
        """

        if isinstance(step, Step):
            self._steps.append(step)
        else:
            self._steps.append(Step(function, argument_dict))

    def change_kwarg(self, function_name, kwarg, new_value):
        """
        Changing specific argument of given function into a new value. IMPORTANT: if there are two functions with the
        same name, only the first one will be changed. If you want to change another one, you should do this using
        indexing and altering single Step 'argument' parameter.
        """
        if function_name not in [func.__name__ for func, _ in self._steps]:
            raise KeyError(f"Function {function_name} not in steps.")
        self[function_name].change_kwarg(kwarg, new_value)

    def apply_to(self, obj):
        """
        Running all steps on a given object. Same as simply calling the Steps.
        """
        for step in self._steps:
            obj = step(obj)
        return obj

    def get_run_times(self):
        """
        If steps has been already called, you can check the run times of specific steps or total time elapsed.
        """

        run_time_dict = \
            {f'{step_no:02d}-{step.function.__name__}': step.elapsed_time for step_no, step in enumerate(self._steps)}

        if any([isinstance(run_time, str) for run_time in run_time_dict.values()]):
            total_time = None
        else:
            total_time = sum(run_time_dict.values())
        return run_time_dict, total_time
