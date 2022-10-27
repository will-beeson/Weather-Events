# R Script to extract weather events from tableau_asset, retain "significant" events by day and asset
# and aggregate to a policy level
# NO loss assumptions at this point
# Intended only to display event and loss assumption can be made based on maximum limit (by locations exposed)

# ---- Library Loads ----

library(odbc)
library(RMySQL)
library(data.table)

## ---- Connect to PRD, pull bound policies and weather events and merge ----

con <- dbConnect(odbc(),
                 Driver = Sys.getenv("SQL_DRIVER"),
                 Server = Sys.getenv("PRD_SERVER"),
                 Database = Sys.getenv("PRD_DATABASE"),
                 UID = Sys.getenv("PRD_USERNAME"),
                 PWD      = Sys.getenv("PRD_PASSWORD"),
                 Port = 3306)

query <- dbSendQuery(con, "SELECT `Policy Reference`, `Insured Full Name`, `Class of Business`, `Location Id`, 
                       `Limit Type`, `Occupancy Description`, `Roof Year`, `Year Built`, `Construction Description`, `Roof Type`, 
                       `County`, `State`, `Location Total Insured Value`, `Original Overlying Deductible`, `Original Underlying Deductible`
                       FROM tableau WHERE `Class of Business` = 'WDBB' AND `Policy Reference` IS NOT NULL" )

data <- as.data.table(dbFetch(query))

query <- dbSendQuery(con, "SELECT * FROM tableau_asset WHERE `Primary Measurement` > 0 " )

wdata <- as.data.table(dbFetch(query))
wdata <- wdata[,lapply(.SD,max), by = c("Asset Id", "Date", "Event", "Measurement Type", "Primary Unit", "Storm Name"), .SDcols = "Primary Measurement"]
# take maximum primary measurement for unique combination of event, date, event and measurement type


ldata <- merge(wdata, data, by.x = 'Asset Id', by.y = 'Location Id', all.x = TRUE)


# ---- Feed in parameters and define damage %s ----

# TODO set damage parameters and loads for rating factors

loss_rate <- 1

# ---- Apply Loss at location level ----

#set deductible at a location level

ldata[, GU_Loss := loss_rate * `Location Total Insured Value`]
ldata[`Original Underlying Deductible` < 1 & `Limit Type` == "Per Location" , `Location Deductible` := `Original Underlying Deductible` * `Location Total Insured Value`]
ldata[`Original Underlying Deductible` > 1 & `Limit Type` == "Per Location" , `Location Deductible` := `Original Underlying Deductible` ]
ldata[`Limit Type` != "Per Location" , `Location Deductible` := 0]

ldata[`Original Overlying Deductible` < 1 & `Limit Type` != "Per Ocurrence" , `Location Limit` := `Original Overlying Deductible` * `Location Total Insured Value`]
ldata[`Original Overlying Deductible` >= 1 & `Limit Type` != "Per Ocurrence" , `Location Limit` := `Original Overlying Deductible`]
ldata[`Limit Type` == "Per Occurrence", `Location Limit` := `Location Total Insured Value`]


ldata[, `Location Loss` := pmin(GU_Loss , `Location Limit`)]
ldata[, `Location Loss` := pmax(`Location Loss` - `Location Deductible`,0)]

#policy loss data
pldata <- ldata[, lapply(.SD, sum), by = c("Date", "Measurement Type", "Policy Reference", "Insured Full Name", "Class of Business", 
                                           "Limit Type", "Original Overlying Deductible", "Original Underlying Deductible"),
                .SDcols = c("Location Loss", "Location Total Insured Value")]

pldata[`Limit Type` != "Per Location" & `Original Underlying Deductible` < 1, `Occurrence Deductible` := `Original Underlying Deductible` * `Location Total Insured Value`]
pldata[`Limit Type` != "Per Location" & `Original Underlying Deductible` > 1, `Occurrence Deductible` := `Original Underlying Deductible` ]
pldata[`Limit Type` == "Per Location" , `Occurrence Deductible` := 0 ]

pldata[`Limit Type` == "Per Occurrence" & `Original Overlying Deductible` < 1, `Occurrence Limit` := `Original Overlying Deductible` * `Location Total Insured Value`]
pldata[`Limit Type` == "Per Occurrence" & `Original Overlying Deductible` > 1, `Occurrence Limit` := `Original Overlying Deductible` ]
pldata[`Limit Type` != "Per Occurrence" , `Occurrence Limit` := `Location Total Insured Value` ]

pldata[, `Occurrence Loss` := pmin(`Location Loss` , `Occurrence Limit`)]
pldata[, `Occurrence Loss` := pmax(`Occurrence Loss` - `Occurrence Deductible` , 0)]

write.csv(pldata, file = "output_policy_loss_data")
write.csv(ldata, file = "location_loss_data")