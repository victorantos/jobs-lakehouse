"""Enum decode tables for the career.coffee source system.

The original platform (career.coffee) serialises C# enums as their INTEGER
ordinals — both in its API JSON and in its SQL Server columns. The newer
sibling boards (solar, dental, …) store strings instead. So bronze rows from
coffee carry e.g. employmentType=0 while solar rows carry jobType="FullTime".

These maps are transcribed from the source of truth:
  CareerCoffee/backend/src/CareerCoffee.Api/Models/CoffeeEnums.cs
C# enums without explicit values number from 0 in declaration order, and that
file marks them append-only, so the ordinals below are stable.

Silver (stage 2) materialises these as small reference tables and joins them
to decode the ints — the lakehouse equivalent of a dimension/lookup table,
kept in code so the repo is self-contained and the mapping is reviewable.
"""

# CoffeeRoleType — the role taxonomy of the original board
COFFEE_ROLE_TYPE = {
    0: "Barista",
    1: "HeadBarista",
    2: "Roaster",
    3: "HeadRoaster",
    4: "GreenBuyer",
    5: "QGrader",
    6: "Farmer",
    7: "FarmManager",
    8: "EquipmentTech",
    9: "CafeOwner",
    10: "CafeManager",
    11: "Trainer",
    12: "Consultant",
    13: "SalesRep",
    14: "Importer",
    15: "Exporter",
    16: "Distributor",
    17: "Agronomist",
    18: "LabTechnician",
    19: "ProductionManager",
    20: "Other",
}

# JobCategory — coarse grouping used for browse/filter UX
COFFEE_JOB_CATEGORY = {
    0: "BaristaAndCafeStaff",
    1: "RoastingAndProduction",
    2: "GreenCoffeeAndTrading",
    3: "QualityControl",
    4: "EducationAndTraining",
    5: "SalesAndMarketing",
    6: "FarmAndOrigin",
    7: "EquipmentAndTechnical",
    8: "ManagementAndOperations",
}

# EmploymentType — note the STRING versions of these same names are what the
# newer boards store in their jobType column, so this list doubles as the
# conformance vocabulary for employment type across all seven boards.
EMPLOYMENT_TYPE = {
    0: "FullTime",
    1: "PartTime",
    2: "Contract",
    3: "Freelance",
    4: "Internship",
    5: "Seasonal",
    6: "Temporary",
}

# JobListingStatus — we export Active (1) only, but bronze keeps whatever
# arrives, so the decode map covers the full range.
JOB_LISTING_STATUS = {
    0: "Draft",
    1: "Active",
    2: "Paused",
    3: "Filled",
    4: "Expired",
    5: "Closed",
}

# CompanyType — company classification on career.coffee
COMPANY_TYPE = {
    0: "Cafe",
    1: "Roastery",
    2: "CafeRoastery",
    3: "Importer",
    4: "Exporter",
    5: "Farm",
    6: "Cooperative",
    7: "EquipmentManufacturer",
    8: "EquipmentDistributor",
    9: "TrainingCenter",
    10: "Consultancy",
    11: "Laboratory",
    12: "Bistro",
    13: "Restaurant",
    14: "Bakery",
    15: "Hotel",
}
