--------------STAGING------------------------
DROP TABLE IF EXISTS sttgaz.stage_optimatica_YearPlan;

CREATE TABLE sttgaz.stage_optimatica_YearPlan (
    "Id" VARCHAR(100) NOT NULL,
    "Number" VARCHAR(30) NOT NULL,
    "ObjectType_Code" VARCHAR(100),
    "ObjectType_Name" VARCHAR(300),
    "ObjectClass_Code" VARCHAR(100),
    "ObjectClass_Name" VARCHAR(300),
    "CreatedAt" TIMESTAMP WITH TIME ZONE,
    "CreatedBy" VARCHAR(300),
    "Deleted" VARCHAR(100),
    "Frozen" VARCHAR(100),
    "Dealer" VARCHAR(6000),
    "Dealer_Id" VARCHAR(100),
    "DealerCity" VARCHAR(6000),
    "DealerLegalAddress" VARCHAR(6000),
    "Year" VARCHAR(50),
    "PeriodFrom" TIMESTAMP WITH TIME ZONE,
    "PeriodTo" TIMESTAMP WITH TIME ZONE,
    "Specialization" VARCHAR(300),
    "ZoneAM" VARCHAR(300),
    "RegionalSalesManager" VARCHAR(300),
    "MinimumBudget" REAL,
    "PlanBudget" REAL,
    "FactBudget" REAL,
    "Budget" REAL,
    "YearPlanItems" VARCHAR(6000),
    "State" VARCHAR(100),
    "Deadline" TIMESTAMP,
    "Activity" TIMESTAMP,
    "ts" TIMESTAMP,
    
    CONSTRAINT YearPlan_id_unique UNIQUE (Id) ENABLED
)
ORDER BY "Id", "Dealer"
SEGMENTED BY hash("Id") ALL NODES
PARTITION BY EXTRACT(year FROM PeriodFrom AT TIME ZONE 'Europe/Moscow');

DROP TABLE IF EXISTS sttgaz.stage_optimatica_QuarterPlan;

CREATE TABLE sttgaz.stage_optimatica_QuarterPlan (
    "Id" VARCHAR(100) NOT NULL,
    "Number" VARCHAR(30) NOT NULL,
    "ObjectType_Code" VARCHAR(100),
    "ObjectType_Name" VARCHAR(300),
    "ObjectClass_Code" VARCHAR(100),
    "ObjectClass_Name" VARCHAR(300),
    "CreatedAt" TIMESTAMP WITH TIME ZONE,
    "CreatedBy" VARCHAR(300),
    "Deleted" VARCHAR(100),
    "Frozen" VARCHAR(100),
    "Dealer" VARCHAR(6000),
    "Dealer_Id" VARCHAR(100),
    "DealerCity" VARCHAR(6000),
    "Quarter" VARCHAR(50),
    "PeriodFrom" TIMESTAMP WITH TIME ZONE,
    "PeriodTo" TIMESTAMP WITH TIME ZONE,
    "Specialization" VARCHAR(300),
    "ZoneAM" VARCHAR(300),
    "RegionalSalesManager" VARCHAR(300),
    "PlanBudget" REAL,
    "QuarterBudget" REAL,
    "QuarterPlanItems" VARCHAR(6000),
    "State" VARCHAR(100),
    "Deadline" TIMESTAMP,
    "Activity" TIMESTAMP,
    "ts" TIMESTAMP,
    
    CONSTRAINT QuarterPlan_id_unique UNIQUE (Id) ENABLED
)
ORDER BY "Id", "Dealer"
SEGMENTED BY hash("Id") ALL NODES
PARTITION BY EXTRACT(YEAR FROM PeriodFrom AT TIME ZONE 'Europe/Moscow');


DROP TABLE IF EXISTS sttgaz.stage_optimatica_MinimumBudget;

CREATE TABLE sttgaz.stage_optimatica_MinimumBudget (
    "Id" VARCHAR(100) NOT NULL,
    "Number" VARCHAR(30) NOT NULL,
    "ObjectType_Code" VARCHAR(100),
    "ObjectType_Name" VARCHAR(300),
    "ObjectClass_Code" VARCHAR(100),
    "ObjectClass_Name" VARCHAR(300),
    "CreatedAt" TIMESTAMP WITH TIME ZONE,
    "CreatedBy" VARCHAR(300),
    "Deleted" VARCHAR(100),
    "Frozen" VARCHAR(100),
    "Dealer" VARCHAR(6000),
    "Dealer_Id" VARCHAR(100),
    "DealerCity" VARCHAR(6000),
    "Year" VARCHAR(50),
    "PeriodFrom" TIMESTAMP WITH TIME ZONE,
    "PeriodTo" TIMESTAMP WITH TIME ZONE,
    "Specialization" VARCHAR(300),
    "ZoneAM" VARCHAR(300),
    "RegionalSalesManager" VARCHAR(300),
    "TotalBudget" REAL,
    "ts" TIMESTAMP,
    
    CONSTRAINT MinimumBudget_id_unique UNIQUE (Id) ENABLED
)
ORDER BY "Id", "Dealer"
SEGMENTED BY hash("Id") ALL NODES
PARTITION BY EXTRACT(YEAR FROM PeriodFrom AT TIME ZONE 'Europe/Moscow');

DROP TABLE IF EXISTS sttgaz.stage_optimatica_YearPlanItem;

CREATE TABLE sttgaz.stage_optimatica_YearPlanItem (
    "Id" VARCHAR(100) NOT NULL,
    "Number" VARCHAR(30) NOT NULL,
    "ObjectType_Code" VARCHAR(100),
    "ObjectType_Name" VARCHAR(300),
    "ObjectClass_Code" VARCHAR(100),
    "ObjectClass_Name" VARCHAR(300),
    "CreatedAt" TIMESTAMP WITH TIME ZONE,
    "CreatedBy" VARCHAR(300),
    "Deleted" VARCHAR(100),
    "Frozen" VARCHAR(100),
    "Month"  VARCHAR(30),
    "PeriodFrom" TIMESTAMP WITH TIME ZONE,
    "PeriodTo" TIMESTAMP WITH TIME ZONE,
    "Media" VARCHAR(6000),
    "Model" VARCHAR(6000),
    "TotalPrice" REAL,
    "Plan_Id" VARCHAR(6000),
    "ts" TIMESTAMP,
    
    CONSTRAINT YearPlanItem_id_unique UNIQUE (Id) ENABLED
)
ORDER BY "Plan_Id", "PeriodFrom"
SEGMENTED BY hash("Id") ALL NODES
PARTITION BY EXTRACT(YEAR FROM PeriodFrom AT TIME ZONE 'Europe/Moscow');


DROP TABLE IF EXISTS sttgaz.stage_optimatica_QuarterPlanItem;

CREATE TABLE sttgaz.stage_optimatica_QuarterPlanItem (
    "Id" VARCHAR(100) NOT NULL,
    "Number" VARCHAR(30) NOT NULL,
    "ObjectType_Code" VARCHAR(100),
    "ObjectType_Name" VARCHAR(300),
    "ObjectClass_Code" VARCHAR(100),
    "ObjectClass_Name" VARCHAR(300),
    "CreatedAt" TIMESTAMP WITH TIME ZONE,
    "CreatedBy" VARCHAR(300),
    "Deleted" VARCHAR(100),
    "Frozen" VARCHAR(100),
    "PeriodFrom" TIMESTAMPTZ,
    "PeriodTo" TIMESTAMPTZ,
    "Model" VARCHAR(6000),
    "Media" VARCHAR(6000),
    "Description" VARCHAR(6000),
    "Price" REAL,
    "QuarterPlan_Id" VARCHAR(6000),
    "ts" TIMESTAMP,
    
    CONSTRAINT QuarterPlanItem_id_unique UNIQUE (Id) ENABLED
)
ORDER BY "QuarterPlan_Id", "PeriodFrom"
SEGMENTED BY hash("Id") ALL NODES
PARTITION BY EXTRACT(YEAR FROM PeriodFrom AT TIME ZONE 'Europe/Moscow');



DROP TABLE IF EXISTS sttgaz.stage_optimatica_Placement;

CREATE TABLE sttgaz.stage_optimatica_Placement (
    "Id" VARCHAR(100) NOT NULL,
    "Number" VARCHAR(30) NOT NULL,
    "ObjectType_Code" VARCHAR(100),
    "ObjectType_Name" VARCHAR(300),
    "ObjectClass_Code" VARCHAR(100),
    "ObjectClass_Name" VARCHAR(300),
    "CreatedAt" TIMESTAMP WITH TIME ZONE,
    "CreatedBy" VARCHAR(300),
    "Deleted" VARCHAR(100),
    "Frozen" VARCHAR(100),
    "Dealer" VARCHAR(6000),
    "Dealer_Id" VARCHAR(100),
    "PeriodFrom" TIMESTAMP WITH TIME ZONE,
    "PeriodTo" TIMESTAMP WITH TIME ZONE,
    "Specialization" VARCHAR(300),
    "ZoneAM" VARCHAR(300),
    "RegionalSalesManager" VARCHAR(300),
    "Media" VARCHAR(6000),
    "Model" VARCHAR(6000),
    "Site" VARCHAR(300),
    "Description" VARCHAR(10000),
    "PublishCount" REAL,
    "MeasureUnit" VARCHAR(1000),
    "QuarterPlanItemRef"  VARCHAR(300),
    "Price" REAL,
    "State" VARCHAR(100),
    "Deadline" TIMESTAMP,
    "Activity" TIMESTAMP,
    "ts" TIMESTAMP,
    
    CONSTRAINT Placement_id_unique UNIQUE (Id) ENABLED
)
ORDER BY "Dealer", "PeriodFrom"
SEGMENTED BY hash("Id") ALL NODES
PARTITION BY EXTRACT(YEAR FROM PeriodFrom);

-------------------DDS---------------------------


DROP TABLE IF EXISTS sttgaz.aux_optimatica_dealers;

CREATE TABLE sttgaz.aux_optimatica_dealers (
    "id" AUTO_INCREMENT PRIMARY KEY,
    "dealer_name" VARCHAR(6000),
    "dealer_id" VARCHAR(100),
    "dealer_city" VARCHAR(6000),
    "dealer_address" VARCHAR(6000),

    CONSTRAINT dealers_dealer_id_unique UNIQUE (dealer_id) ENABLED
)
ORDER BY "id", "dealer_id"
SEGMENTED BY hash("id") ALL NODES;


DROP TABLE IF EXISTS sttgaz.aux_optimatica_year_plans;

CREATE TABLE sttgaz.aux_optimatica_year_plans (
    "id" AUTO_INCREMENT PRIMARY KEY,
    "plan_id" VARCHAR(100) NOT NULL,
    "deleted" VARCHAR(100),
    "frozen" VARCHAR(100),
    "created_at" TIMESTAMP WITH TIME ZONE,
    "dealer_id" INT REFERENCES sttgaz.aux_optimatica_dealers(id),
    "year" VARCHAR(50),
    "period_from" DATE,
    "period_to" DATE,
    "specialization" VARCHAR(300),
    "minimum_budget" REAL,
    "plan_budget" REAL,
    "fact_budget" REAL,
    "budget" REAL,
    "state" VARCHAR(100),

    CONSTRAINT plan_id_unique UNIQUE (plan_id) ENABLED
)
ORDER BY "id", "dealer_id", "plan_id"
SEGMENTED BY hash("id") ALL NODES
PARTITION BY "year";


DROP TABLE IF EXISTS sttgaz.aux_optimatica_year_plan_items;

CREATE TABLE sttgaz.aux_optimatica_year_plan_items (
    "id" AUTO_INCREMENT PRIMARY KEY,
    "item_id" VARCHAR(100) NOT NULL,
    "plan_id"  INT REFERENCES sttgaz.aux_optimatica_year_plans(id),
    "created_at" TIMESTAMP WITH TIME ZONE,
    "deleted" VARCHAR(100),
    "frozen" VARCHAR(100),
    "month"  VARCHAR(30),
    "period_from" DATE,
    "period_to" DATE,
    "media" VARCHAR(6000),
    "model" VARCHAR(6000),
    "total_price" REAL,
    
    CONSTRAINT item_id_unique UNIQUE (item_id) ENABLED
)
ORDER BY "plan_id", "media", "model"
SEGMENTED BY hash("id") ALL NODES
PARTITION BY "month";


DROP TABLE IF EXISTS sttgaz.aux_optimatica_placements;

CREATE TABLE sttgaz.aux_optimatica_placements (
    "id" AUTO_INCREMENT PRIMARY KEY,
    "placement_id" VARCHAR(100) NOT NULL,
    "created_at" TIMESTAMP WITH TIME ZONE,
    "deleted" VARCHAR(100),
    "frozen" VARCHAR(100),
    "dealer_id" INT REFERENCES sttgaz.aux_optimatica_dealers(id),
    "period_from" DATE,
    "period_to" DATE,
    "specialization" VARCHAR(300),
    "media" VARCHAR(6000),
    "model" VARCHAR(6000),
    "site" VARCHAR(300),
    "description" VARCHAR(10000),
    "publish_count" REAL,
    "measure_unit" VARCHAR(1000),
    "price" REAL,
    "state" VARCHAR(100),
    
    CONSTRAINT placement_id_unique UNIQUE (placement_id) ENABLED
)
ORDER BY "dealer_id", "media", "model"
SEGMENTED BY hash("id") ALL NODES
PARTITION BY "dealer_id";


DROP TABLE IF EXISTS sttgaz.aux_optimatica_calendar;

CREATE TABLE sttgaz.aux_optimatica_calendar ( 
     "month"      DATE
);