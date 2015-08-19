from pyomo.environ import *
import switch_mod.utilities as utilities


def define_components(m):
    """Make various changes to the model to facilitate reporting and avoid unwanted behavior"""
    
    # define an indexed set of all periods before or including the current one.
    # this is useful for calculations that must index over previous and current periods
    # e.g., amount of capacity of some resource that has been built
    m.CURRENT_AND_PRIOR_PERIODS = Set(m.PERIODS, ordered=True, initialize=lambda m, p:
        # note: this is a fast way to refer to all previous periods, which also respects 
        # the built-in ordering of the set, but you have to be careful because 
        # (a) pyomo sets are indexed from 1, not 0, and
        # (b) python's range() function is not inclusive on the top end.
        [m.PERIODS[i] for i in range(1, m.PERIODS.ord(p)+1)]
    )
    
    # define dispatch-related components with values for all timepoints.
    # This simplifies and strongly accelerates operations that need to calculate totals
    # across sets of projects for a given timepoint.
    m.DispatchProj_AllTimePoints = Expression(
        m.PROJECTS, m.TIMEPOINTS, 
        rule=lambda m, p, t:
            m.DispatchProj[p, t] if (p, t) in m.PROJ_DISPATCH_POINTS
                else 0.0
    )
    m.DispatchUpperLimit_AllTimePoints = Expression(
        m.PROJECTS, m.TIMEPOINTS, 
        rule=lambda m, p, t:
            m.DispatchUpperLimit[p, t] if (p, t) in m.PROJ_DISPATCH_POINTS
                else 0.0
    )
    # speed up definition of LZ_NetDispatch in the core switch model
    def LZ_NetDispatch_fast_rule(m, lz, t):
        if not m.DispatchProj_AllTimePoints._constructed:
            # construct the DispatchProj_AllTimePoints Expression the first time through.
            m.DispatchProj_AllTimePoints.construct()
            # This is necessary because DispatchProj_AllTimePoints is defined in this
            # module, which is is loaded after all the standard switch modules. 
            # So DispatchProj_AllTimePoints is normally scheduled for construction after 
            # all the standard module components. But this rule needs to use it during 
            # construction of LZ_NetDispatch, which is part of the standard modules.
            # An alternative approach, which is both cleaner and messier, would be to 
            # manipulate m._decl_order to move DispatchProj_AllTimePoints up in the 
            # construction order, to just after DispatchProj, as suggested in
            # https://groups.google.com/d/msg/pyomo-forum/dLbD2ly_hZo/5-INUaECNBkJ
        return sum(m.DispatchProj_AllTimePoints[p, t] for p in m.LZ_PROJECTS[lz])
    m.LZ_NetDispatch._init_rule = LZ_NetDispatch_fast_rule


    # create lists of projects by energy source
    m.PROJECTS_BY_FUEL = Set(m.FUELS, initialize=lambda m, f:
        # we sort this to help with display, but that may not actually have any effect
        sorted([p for p in m.FUEL_BASED_PROJECTS if m.proj_fuel[p] == f])
    )
    m.PROJECTS_BY_NON_FUEL_ENERGY_SOURCE = Set(m.NON_FUEL_ENERGY_SOURCES, initialize=lambda m, s:
        # we sort this to help with display, but that may not actually have any effect
        sorted([p for p in m.NON_FUEL_BASED_PROJECTS if m.proj_non_fuel_energy_source[p] == s])
    )

    # constrain DumpPower to zero, so we can track curtailment better
    m.No_Dump_Power = Constraint(m.LOAD_ZONES, m.TIMEPOINTS,
        rule=lambda m, z, t: m.DumpPower[z, t] == 0.0
    )

