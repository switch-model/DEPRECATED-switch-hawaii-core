import os
from pyomo.environ import *

def define_components(m):
    
    # make helper set identifying all timeseries in each period
    if hasattr(m, "PERIOD_TS"):
        print "DEPRECATION NOTE: PERIOD_TS is defined in hydrogen.py, but it already exists, so this can be removed."
    else:
        m.PERIOD_TS = Set(m.PERIODS, ordered=True, within=m.TIMESERIES, initialize=lambda m, p:
            ts for ts in m.TIMESERIES if m.ts_period[t] == p])
        )

    # electrolyzer details
    m.hydrogen_electrolyzer_capital_cost_per_mw = Param()
    m.hydrogen_electrolyzer_fixed_cost_per_mw_year = Param(default=0.0)
    m.hydrogen_electrolyzer_variable_cost_per_kg = Param(default=0.0)  # assumed to include any refurbishment needed
    m.hydrogen_electrolyzer_kg_per_mwh = Param()
    m.hydrogen_electrolyzer_life_hours = Param()
    m.BuildElectrolyzerMW = Var(m.LOAD_ZONES, m.PERIODS, within=NonNegativeReals)
    m.Electrolyzer_Capacity_MW = Expression(m.LOAD_ZONES, rule=lambda m, z: 
        sum(m.BuildElectrolyzerMW[z] for p in m.CURRENT_AND_PRIOR_PERIODS[p]))
    m.RunElectrolyzerMW = Var(m.LOAD_ZONES, m.TIMEPOINTS, within=NonNegativeReals)
    m.Produce_Hydrogen_Kg = Expression(m.LOAD_ZONES, m.TIMEPOINTS, rule=lambda m, z, t:
        m.RunElectrolyzerMW[z, t] * m.ts_duration_of_tp[m.tp_ts[t]] * m.hydrogen_electrolyzer_kg_per_mwh
    )

    # note: we assume there is a compressed hydrogen tank that is big enough to buffer
    # daily production, storage and withdrawals of hydrogen, but we don't include a cost
    # for this (because it will be negligible compared to the rest of the costs)
    # This allows the system to do some intra-day arbitrage without going all the way to liquification

    # liquifier details
    m.hydrogen_liquifier_capital_cost_per_kg_per_hour = Param()
    m.hydrogen_liquifier_fixed_cost_per_kg_hour_year = Param(default=0.0)
    m.hydrogen_liquifier_variable_cost_per_kg = Param(default=0.0)
    m.hydrogen_liquifier_mwh_per_kg = Param()
    m.BuildLiquifierKgHour = Var(m.LOAD_ZONES, m.PERIODS, within=NonNegativeReals)
    m.LiquifyHydrogenKg = Var(m.LOAD_ZONES, m.TIMEPOINTS, within=NonNegativeReals)
    m.LiquifyHydrogenMW = Expression(m.LOAD_ZONES, m.TIMEPOINTS, rule=lambda m, z, t:
        m.LiquifyHydrogenKg[z, t] / m.hydrogen_electrolyzer_kg_per_mwh
    )
    
    # storage tank details
    m.liquid_hydrogen_tank_capital_cost_per_kg = Param()
    m.BuildLiquidHydrogenTankKg = Var(m.LOAD_ZONES, m.PERIODS, within=NonNegativeReals) # in kg
    m.StoreLiquidHydrogenKg = Expression(m.LOAD_ZONES, m.TIMESERIES, rule=lambda m, z, ts:
        sum(m.LiquifyHydrogenKg[z, tp] for tp in m.TS_TPS[tp])
    )
    m.WithdrawLiquidHydrogenKg = Var(m.LOAD_ZONES, m.TIMESERIES, within=NonNegativeReals)
    # note: we assume the system will be large enough to neglect boil-off

    # fuel cell details
    m.hydrogen_fuel_cell_capital_cost_per_mw = Param()
    m.hydrogen_fuel_cell_fixed_cost_per_mw_year = Param(default=0.0)
    m.hydrogen_fuel_cell_variable_cost_per_mwh = Param(default=0.0) # assumed to include any refurbishment needed
    m.hydrogen_fuel_cell_mwh_per_kg = Param()
    m.hydrogen_fuel_cell_life_hours = Param()
    m.BuildFuelCellMW = Var(m.LOAD_ZONES, m.PERIODS, within=NonNegativeReals)
    m.FuelCellCapacityMW = Expression(m.LOAD_ZONES, rule=lambda m, z: 
        sum(m.BuildFuelCellMW[z] for p in m.CURRENT_AND_PRIOR_PERIODS[p]))
    m.DispatchFuelCellMW = Var(m.LOAD_ZONES, m.TIMEPOINTS, within=NonNegativeReals)
    m.Consume_Hydrogen_Kg = Expression(m.LOAD_ZONES, m.TIMEPOINTS, rule=lambda m, z, t:
        m.DispatchFuelCellMW[z, t] * m.ts_duration_of_tp[m.tp_ts[t]] / m.hydrogen_fuel_cell_mwh_per_kg
    )

    # hydrogen mass balances
    m.Hydrogen_Conservation_of_Mass_Daily = Constraint(m.LOAD_ZONES, m.TIMESERIES, rule=lambda m, z, ts:
        m.StoreLiquidHydrogenKg[z, ts] - m.WithdrawLiquidHydrogenKg[z, ts]
        == 
        sum(m.Produce_Hydrogen_Kg[z, tp] - m.Consume_Hydrogen_Kg[z, tp] for tp in m.TS_TPS[ts])
    )
    m.Hydrogen_Conservation_of_Mass_Annual = Constraint(m.LOAD_ZONES, m.PERIODS, rule=lambda m, z, p:
        sum(m.StoreLiquidHydrogenKg[ts] - m.WithdrawLiquidHydrogenKg[ts] for ts in PERIOD_TS[p]) == 0
    )

    # limits on equipment
    m.Max_Run_Electrolyzer = Constraint(m.LOAD_ZONES, m.TIMEPOINTS, rule=lambda m, z, t:
        m.RunElectrolyzerMW[z, t] <= m.Electrolyzer_Capacity_MW[z, m.tp_period[t]])
    m.Max_Run_Electrolyzer = Constraint(m.LOAD_ZONES, m.TIMEPOINTS, rule=lambda m, z, t:
        m.DispatchFuelCellMW[z, t] <= m.FuelCellCapacityMW[z, m.tp_period[t]])
    # there must be enough storage to hold _all_ the production each period (net of same-day consumption)
    # note: this assumes we cycle the system only once per year (store all energy, then release all energy)
    # alternatives: allow monthly or seasonal cycling, or directly model the whole year with inter-day linkages
    m.Max_Store_Liquid_Hydrogen = Constraint(m.LOAD_ZONES, m.PERIODS, rule=lambda m, z, p:
        sum(m.StoreLiquidHydrogenKg[z, ts] * m.ts_scale_to_year[z, ts] for ts in m.PERIOD_TS[p])
        <=
        sum(BuildLiquidHydrogenTankKg[z, p] for p in m.CURRENT_AND_PRIOR_PERIODS[p])
    )
    
    # add electricity consumption and production to the model
    m.LZ_Energy_Components_Consume.append('RunElectrolyzerMW')
    m.LZ_Energy_Components_Consume.append('LiquifyHydrogenMW')
    m.LZ_Energy_Components_Produce.append('DispatchFuelCellMW')

    # add costs to the model
    
    
    # we treat the electrolyzer and fuel cell as infinitely long-lived (so we pay just interest on the loan),
    # but charge a usage fee corresponding to the reduction in life during each cycle 
    # (i.e., enough to restore it to like-new status, on average)
    m.electrolyzer_cost_per_mwh_cycled = Param(initialize = lambda m:
        m.hydrogen_capital_cost_per_mwh_capacity / (m.hydrogen_n_cycles * m.hydrogen_max_discharge)
    )
    m.hydrogen_fixed_cost_per_year = Param(initialize = lambda m:
        m.hydrogen_capital_cost_per_mwh_capacity * m.interest_rate
    )

    m.hydrogen_electrolyzer_life_hours = Param()
    
    # add the hydrogen equipment to the objective function
    m.Hydrogen_Variable_Cost = Expression(m.TIMEPOINTS, rule=lambda m, t:
        m.Produce_Hydrogen_Kg[z, t] * m.hydrogen_electrolyzer_variable_cost_per_kg
        + m.Produce_Hydrogen_Kg[z, t] * m.hydrogen_electrolyzer_variable_cost_per_kg
    
     = Param(default=0.0)  # assumed to include any refurbishment needed
    
        sum(m.hydrogen_cost_per_mwh_cycled * m.DischargeHydrogen[z, t] for z in m.LOAD_ZONES)
    )
    m.Hydrogen_Fixed_Cost_Annual = Expression(m.PERIODS, rule=lambda m, p:
        sum(m.hydrogen_fixed_cost_per_year * m.Hydrogen_Capacity[z, p] for z in m.LOAD_ZONES)
    )
    m.cost_components_tp.append('Hydrogen_Variable_Cost')
    m.cost_components_annual.append('Hydrogen_Fixed_Cost_Annual')
    
    
    
    # number of full cycles the hydrogen can do; we assume shallower cycles do proportionally less damage
    m.hydrogen_n_cycles = Param()
    # maximum depth of discharge
    m.hydrogen_max_discharge = Param()
    # round-trip efficiency
    m.hydrogen_efficiency = Param()
    # fastest time that storage can be emptied (down to max_discharge)
    m.hydrogen_min_discharge_time = Param()

    # amount of hydrogen capacity to build and use
    # TODO: integrate this with other project data, so it can contribute to reserves, etc.
    m.BuildHydrogen = Var(m.LOAD_ZONES, m.PERIODS, within=NonNegativeReals)
    m.Hydrogen_Capacity = Expression(m.LOAD_ZONES, m.PERIODS, rule=lambda m, z, p:
        sum(m.BuildHydrogen[z, pp] for pp in m.CURRENT_AND_PRIOR_PERIODS[p])
    )

    # rate of charging/discharging hydrogen
    m.ChargeHydrogen = Var(m.LOAD_ZONES, m.TIMEPOINTS, within=NonNegativeReals)
    m.DischargeHydrogen = Var(m.LOAD_ZONES, m.TIMEPOINTS, within=NonNegativeReals)

    # storage level at start of each timepoint
    m.HydrogenLevel = Var(m.LOAD_ZONES, m.TIMEPOINTS, within=NonNegativeReals)


    # Calculate the state of charge based on conservation of energy
    # NOTE: this is circular for each day
    # NOTE: the overall level for the day is free, but the levels each timepoint are chained.
    m.Hydrogen_Level_Calc = Constraint(m.LOAD_ZONES, m.TIMEPOINTS, rule=lambda m, z, t:
        m.HydrogenLevel[z, t] == 
            m.HydrogenLevel[z, m.tp_previous[t]]
            + m.hydrogen_efficiency * m.ChargeHydrogen[z, m.tp_previous[t]] 
            - m.DischargeHydrogen[z, m.tp_previous[t]]
    )
      
    # limits on storage level
    m.Hydrogen_Min_Level = Constraint(m.LOAD_ZONES, m.TIMEPOINTS, rule=lambda m, z, t: 
        (1.0 - m.hydrogen_max_discharge) * m.Hydrogen_Capacity[z, m.tp_period[t]]
        <= 
        m.HydrogenLevel[z, t]
    )
    m.Hydrogen_Max_Level = Constraint(m.LOAD_ZONES, m.TIMEPOINTS, rule=lambda m, z, t: 
        m.HydrogenLevel[z, t]
        <= 
        m.Hydrogen_Capacity[z, m.tp_period[t]]
    )

    m.Hydrogen_Max_Charge = Constraint(m.LOAD_ZONES, m.TIMEPOINTS, rule=lambda m, z, t:
        m.ChargeHydrogen[z, t]
        <=
        m.Hydrogen_Capacity[z, m.tp_period[t]] * m.hydrogen_max_discharge / m.hydrogen_min_discharge_time
    )
    m.Hydrogen_Max_Disharge = Constraint(m.LOAD_ZONES, m.TIMEPOINTS, rule=lambda m, z, t:
        m.DischargeHydrogen[z, t]
        <=
        m.Hydrogen_Capacity[z, m.tp_period[t]] * m.hydrogen_max_discharge / m.hydrogen_min_discharge_time
    )


def load_inputs(mod, switch_data, inputs_dir):
    """
    Import hydrogen data from a .dat file. 
    TODO: change this to allow multiple storage technologies.
    """
    switch_data.load(filename=os.path.join(inputs_dir, 'hydrogens.dat'))
