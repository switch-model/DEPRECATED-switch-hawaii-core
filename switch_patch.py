from pyomo.environ import *
import switch_mod.utilities as utilities


def define_components(m):
    """Make various changes to the model to facilitate reporting and avoid unwanted behavior"""
    
    # define dispatch-related components with values for all timepoints. 
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

