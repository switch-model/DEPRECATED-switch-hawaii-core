import csv, sys, time, itertools
from pyomo.environ import value
import __main__ as main

# check whether this is an interactive session
# (if not, there will be no __main__.__file__)
interactive_session = not hasattr(main, '__file__')

csv.register_dialect("ampl-tab", 
    delimiter="\t", 
    lineterminator="\n",
    doublequote=False, escapechar="\\", 
    quotechar='"', quoting=csv.QUOTE_MINIMAL,
    skipinitialspace = False
)

def create_table(**kwargs):
    """Create an empty output table and write the headings."""
    output_file = kwargs["output_file"]
    headings = kwargs["headings"]

    with open(output_file, 'wb') as f:
        w = csv.writer(f, dialect="ampl-tab")
        # write header row
        w.writerow(list(headings))

def append_table(model, *indexes, **kwargs):
    """Add rows to an output table, iterating over the indexes specified, 
    and getting row data from the values function specified."""
    output_file = kwargs["output_file"]
    values = kwargs["values"]

    # create a master indexing set 
    # this is a list of lists, even if only one list was specified
    idx = itertools.product(*indexes)
    with open(output_file, 'ab') as f:
        w = csv.writer(f, dialect="ampl-tab")
        # write the data
        # import pdb
        # if 'rfm' in output_file:
        #     pdb.set_trace()
        w.writerows(
            tuple(value(v) for v in values(model, *unpack_elements(x))) 
            for x in idx
        )

def unpack_elements(tup):
    """Unpack any multi-element objects within tup, to make a single flat tuple.
    Note: this is not recursive.
    This is used to flatten the product of a multi-dimensional index with anything else."""
    l=[]
    for t in tup:
        if isinstance(t, basestring):
            l.append(t)
        else:
            try:
                # check if it's iterable
                iterator = iter(t)
                for i in iterator:
                    l.append(i)
            except TypeError:
                l.append(t)
    return tuple(l)

def write_table(model, *indexes, **kwargs):
    """Write an output table in one shot - headers and body."""
    output_file = kwargs["output_file"]

    print "Writing {file} ...".format(file=output_file),
    sys.stdout.flush()  # display the part line to the user
    start=time.time()

    create_table(**kwargs)
    append_table(model, *indexes, **kwargs)

    print "time taken: {dur:.2f}s".format(dur=time.time()-start)

def get(component, index, default=None):
    """Return an element from an indexed component, or the default value if the index is invalid."""
    return component[index] if index in component else default
    
def log(msg):
    sys.stdout.write(msg)
    sys.stdout.flush()  # display output to the user, even a partial line
    
def tic():
    tic.start_time = time.time()

def toc():
    log("time taken: {dur:.2f}s\n".format(dur=time.time()-tic.start_time))


import argparse

def iterify(item):
    """Return an iterable for the one or more items passed."""
    if isinstance(item, basestring):
        i = iter([item])
    else:
        try:
            # check if it's iterable
            i = iter(item)
        except TypeError:
            i = iter([item])
    return i

class AddModuleAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        for m in iterify(values):
            setattr(namespace, m, True)

class RemoveModuleAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        for m in iterify(values):
            setattr(namespace, m, False)

class AddListAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        if getattr(namespace, self.dest) is None:
            setattr(namespace, self.dest, list())
        getattr(namespace, self.dest).extend(iterify(values))

# define a standard argument parser, which can be used to setup scenarios
parser = argparse.ArgumentParser(description='Solve one or more Switch-Hawaii scenarios.')
parser.add_argument('--inputs', dest='inputs_dir')
parser.add_argument('--outputs', dest='outputs_dir')
parser.add_argument('--scenario', action=AddListAction, dest='scenario_to_run')
parser.add_argument('--scenarios', action=AddListAction, nargs='+', dest='scenario_to_run')
parser.add_argument('--scenario_name')
parser.add_argument('--tag')
parser.add_argument('--ph_year', type=int)
parser.add_argument('--ph_mw', type=float)
# TODO: something about dr_shares
parser.add_argument('--exclude', action=AddModuleAction, dest='exclude_module', nargs='+')
parser.add_argument('-n', action=RemoveModuleAction, dest='exclude_module')
parser.add_argument('--include', action=AddModuleAction, dest='include_module', nargs='+')
parser.add_argument('-y', action=AddModuleAction, dest='include_module')
parser.add_argument(action=AddModuleAction, dest='include_module', nargs='*')

def args_dict(*a):
    """call the parser to get the args, then return them as a dictionary, omitting None's'"""
    return {k: v for k, v in vars(parser.parse_args(*a)).iteritems() if v is not None}

def parse_scenario_list(scenarios):
    """Convert a list of scenarios (specified in the form of a command line string) 
    into a list of argument dictionaries."""
    scenario_list = []
    for s in scenarios:
        # parse scenario arguments
        a = args_dict(s.split())
        scenario_list.append(a)
    return scenario_list

def adjust_scenarios(scenarios):
    """Apply command line arguments to a previously parsed list of standard scenarios and return the new list."""
    args = args_dict()  # get command line arguments
    return [merge_scenarios(s, args) for s in scenarios]
    
def adjust_scenario(scenario):
    """Apply command line arguments to a previously parsed scenario and return the scenario definition."""
    args = args_dict()  # get command line arguments
    return merge_scenarios(scenario, args)

def requested_scenarios(standard_scenarios):
    """Return a list of argument dictionaries defining scenarios requested from the command line 
    (possibly drawn/modified from the specified list of standard_scenarios)."""
    args = args_dict()    
    requested_scenarios = []
    standard_dict = {s["scenario_name"]: s for s in standard_scenarios}
    if "scenario_to_run" in args:
        for s in args["scenario_to_run"]:
            if s not in standard_dict:
                raise RuntimeError("scenario {s} has not been defined.".format(s=s))
            else:
                # note: if they specified a scenario_name here, it will override the standard one
                requested_scenarios.append(merge_scenarios(standard_dict[s], args))
    elif "scenario_name" in args:
        # they have defined one specific scenario on the command line
        requested_scenarios.append(args)
    return requested_scenarios


def merge_scenarios(*scenarios):
    # combine scenarios: start with the first and then apply most settings from later ones
    # but concatenate "tag" entries and remove "scenario_to_run" entries
    d = dict(tag='')
    for s in scenarios:
        t1 = d["tag"]
        t2 = s.get("tag", "")
        s["tag"] = t1 + ("" if t1 == "" or t2 == "" else "_") + t2
        d.update(s)
    if 'scenario_to_run' in d:
        del d['scenario_to_run']
    return d

