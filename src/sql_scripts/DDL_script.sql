

CREATE TABLE "factCovid" (
"index" INTEGER,
  "fips" REAL,
  "province_state" TEXT,
  "country_region" TEXT,
  "confirmed" REAL,
  "deaths" REAL,
  "recovered" REAL,
  "active" REAL,
  "date" INTEGER,
  "positive" REAL,
  "negative" REAL,
  "hospitalizedcurrently" REAL,
  "hospitalized" REAL,
  "hospitalizeddischarged" REAL
);


CREATE TABLE "dimHospital" (
"index" INTEGER,
  "fips" REAL,
  "state_name" TEXT,
  "latitude" REAL,
  "longtitude" REAL,
  "hq_address" TEXT,
  "hospital_name" TEXT,
  "hospital_type" TEXT,
  "hq_city" TEXT,
  "hq_state" TEXT
);


CREATE TABLE "dimRegion" (
"index" INTEGER,
  "fips" REAL,
  "province_state" TEXT,
  "country_region" TEXT,
  "latitude" REAL,
  "longitude" REAL,
  "county" TEXT,
  "state" TEXT
);
CREATE TABLE "dimDate" (
"index" INTEGER,
  "fips" INTEGER,
  "date" TIMESTAMP,
  "month" INTEGER,
  "year" INTEGER,
  "day_of_week" INTEGER
);