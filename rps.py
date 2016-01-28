import os
from pprint import pprint
from pyomo.environ import *
import switch_mod.utilities as utilities
from util import get

def define_components(m):
    """

    """
    ###################
    # RPS calculation
    ##################
    
    m.f_rps_eligible = Param(m.FUELS, within=Binary)

    m.RPS_ENERGY_SOURCES = Set(initialize=lambda m: 
        list(m.NON_FUEL_ENERGY_SOURCES) + [f for f in m.FUELS if m.f_rps_eligible[f]])

    m.RPS_YEARS = Set(ordered=True)
    m.rps_target = Param(m.RPS_YEARS)

    def rps_target_for_period_rule(m, p):
        """find the last target that is in effect before the _end_ of the period"""
        latest_target = max(y for y in m.RPS_YEARS if y <= m.period_end[p])
        return m.rps_target[latest_target]
    m.rps_target_for_period = Param(m.PERIODS, initialize=rps_target_for_period_rule)

    # maximum share of (bio)fuels in rps
    # note: using Infinity as the upper limit causes the solution to take forever
    # m.rps_fuel_limit = Param(default=float("inf"), mutable=True)
    m.rps_fuel_limit = Param(default=1.0, mutable=True)

    # Note: this rule ignores pumped hydro, so it could be gamed by producing extra 
    # RPS-eligible power and burning it off in storage losses; on the other hand, 
    # it also neglects the (small) contribution from net flow of pumped hydro projects.
    # TODO: incorporate pumped hydro into this rule, maybe change the target to refer to 
    # sum(getattr(m, component)[lz, t] for lz in m.LOAD_ZONES) for component in m.LZ_Energy_Components_Produce)
    # TODO: consider day weights in calculation

    # power production that can be counted toward the RPS each period
    m.RPSEligiblePower = Expression(m.PERIODS, rule=lambda m, per:
        sum(
            m.DispatchProjByFuel[p, t, f] 
                for f in m.FUELS if f in m.RPS_ENERGY_SOURCES
                    for p in m.PROJECTS_BY_FUEL[f]
                        # could be accelerated a bit if we had m.ACTIVE_PERIODS_FOR_PROJECT[p]
                        for t in m.PERIOD_TPS[per]
                            if (p, t) in m.PROJ_DISPATCH_POINTS
        )
        +
        sum(
            m.DispatchProj[p, t]
                for f in m.NON_FUEL_ENERGY_SOURCES if f in m.RPS_ENERGY_SOURCES
                    for p in m.PROJECTS_BY_NON_FUEL_ENERGY_SOURCE[f]
                        for t in m.PERIOD_TPS[per]
                            if (p, t) in m.PROJ_DISPATCH_POINTS
        )
        -
        # assume DumpPower is curtailed renewable energy
        sum(m.DumpPower[lz, tp] for lz in m.LOAD_ZONES for tp in m.PERIOD_TPS[per])
    )

    # total power production each period (against which RPS is measured)
    # (we subtract DumpPower, because that shouldn't have been produced in the first place)
    m.RPSTotalPower = Expression(m.PERIODS, rule=lambda m, per:
        sum(
            m.DispatchProj[p, t] 
                for p in m.PROJECTS 
                    for t in m.PERIOD_TPS[per] 
                        if (p, t) in m.PROJ_DISPATCH_POINTS
        )
        - sum(m.DumpPower[lz, tp] for lz in m.LOAD_ZONES for tp in m.PERIOD_TPS[per])
    )
    
    m.RPS_Enforce = Constraint(m.PERIODS, rule=lambda m, per:
        m.RPSEligiblePower[per] >= m.rps_target_for_period[per] * m.RPSTotalPower[per]
    )

    # Don't allow (bio)fuels to provide more than a certain percentage of the system's energy
    # But note: when the system really wants to use more biofuel, this can be "gamed" by
    # cycling power through batteries or transmission lines to burn off some extra non-fuel energy,
    # allowing more biofuel into the system. One solution would be to only apply the RPS to
    # the predefined load (not generation), but then transmission and battery losses could be
    # served by fossil fuels.
    # Alternatively: limit fossil fuels to (1-rps) * standard loads 
    # and limit biofuels to (1-bio)*standard loads. This would force renewables to be used for
    # all losses, which is slightly inaccurate.
    # Also note: this problem may go away when the RPS and fuel limite are weighted correctly;
    # at present, peak days get small weight for costs and large weight for fuel limit, so there's
    # an excessive incentive to increase non-fuel renewable consumption on these days.
    # TODO: fix the problems noted above; for now we don't worry too much because the cost of
    # burning up excess RE in batteries in order to incorporate more biofuel is very high, and
    # it mainly occurs in scenarios with no other options (no demand response, no hydrogen, etc.)
    m.RPS_Fuel_Cap = Constraint(m.PERIODS, rule = lambda m, per:
        sum(
            m.DispatchProjByFuel[p, t, f] 
                for f in m.FUELS if m.f_rps_eligible[f]
                    for p in m.PROJECTS_BY_FUEL[f]
                        # could be accelerated a bit if we had m.ACTIVE_PERIODS_FOR_PROJECT[p]
                        for t in m.PERIOD_TPS[per]
                            if (p, t) in m.PROJ_DISPATCH_POINTS
        )
        <= m.rps_fuel_limit * m.RPSTotalPower[per]
    )



def load_inputs(m, switch_data, inputs_dir):
    switch_data.load_aug(
        optional=True,
        filename=os.path.join(inputs_dir, 'fuels.tab'),
        select=('fuel', 'rps_eligible'),
        param=(m.f_rps_eligible,))
    switch_data.load_aug(
        optional=True,
        filename=os.path.join(inputs_dir, 'rps_targets.tab'),
        autoselect=True,
        index=m.RPS_YEARS,
        param=(m.rps_target,))

