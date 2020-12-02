class SqlQueries:
    immigration_to_redshift = ("""
        CREATE TABLE IF NOT EXISTS public.immigration (
            i94mon INT,
            cicid INT,
            i94visa INT,
            i94res INT,
            i94yr INT,
            i94mode INT,
            i94cit INT,
            i94bir INT,
            arrdate VARCHAR,
            depdate VARCHAR,
            airline VARCHAR,
            fltno VARCHAR,
            i94port VARCHAR,
            visatype VARCHAR,
            gender VARCHR,
            i94addr VARCAR,
            CONSTRAINT immigration_pkey PRIMARY KEY ("cicid)
        );
    """)

    state_to_redshift = ("""
        CREATE TABLE IF NOT EXISTS public.state (
            Code VARCHAR,
            State VARCHAR,
            BlackOrAfricanAmerican INT,
            White INT,
            ForeignBorn INT,
            AmericanIndianAndAlaskaNative INT,
            HispanicOrLatino INT,
            Asian INT,
            NumberVeterans INT,
            FemalePopulation INT,
            MalePopulation INT,
            TotalPopulation INT,
            CONSTRAINT state_pkey PRIMARY KEY ("Code")
        );
    """)

    date_to_redshift = ("""
        CREATE TABLE public."date" (
            "date" varchar NOT NULL,
            "day" INT,
            "month" INT,
            "year" INT,
            weekofyear INT,
            dayofweek INT,
            CONSTRAINT date_pkey PRIMARY KEY ("date")
        );
    """)