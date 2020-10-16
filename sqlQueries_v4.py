# Specific queries for Links Modular (TM)
# Never make last column of 'select' arg a date column.
# Reference / Lookup Tables
# When modifying the SQL queries - ensure the first selected field is that
# which will be used as Key in Key / Value pairing of a dictionary, i.e.
# Primary Key (unique in the table)

membershipTypes = """
    select ProductCode, ProductId, CategoryId, CategoryDescription,
    CategoryCode, ProductDescription
    FROM MembershipTypesView
    """

classTypes =  """
    SELECT Id, Description, ExtraDescription, ServiceId1, TermMonthly,
    DDStartDate, DDFrequency
    FROM ClassTypes
    """

classTerms = """ 
    SELECT TermId, TermCode, StartDate, EndDate
    FROM ClassTerms
    WHERE StartDate >= GETDATE()
    """

Class_Schedule = """
    /* classes that are valid */
    SELECT ClassTypeId, ClassId, Term, TermID, ClassDesc, StartDate,
    EndDate, LessonDay, BeginTime, EndTime, LessonLevel, TeacherId,
    TeachName
    FROM Class_Schedule
    """

Responsible_Person = """
    
    """

# Health Club
hc = {'birthdays': 
        """
        SELECT CustomerId, Surname, GivenNames, DateOfBirth, Email,
        LastUpdated, CurrentExpiryDate, MembershipTypeId
        FROM MembershipContractsDetails 
        WHERE ProductCode IN (
        'M1050', 'M1059', 'M1061', 'M1063', 'M1074', 
        'M1075', 'M1093', 'M1168', 'M1173', 'M1174', 
        'M1194', 'M1216', 'M1220', 'M1221', 'M1224', 
        'M1225', 'M1228', 'M1229', 'M1237', 'M1238', 
        'M1239', 'M3034', 'M1076', 'M1077', 'M1083', 
        'M1170', 'M1209', 'M1222', 'M1226', 'M1227', 
        'M1230', 'M1231', 'M1232'
        )
        AND
        CurrentExpiryDate >= GETDATE()
        """,
        'upfront_memberships': 
        """
        SELECT * FROM MembershipContractsDetails
        WHERE ProductCode IN (
        'M1050', 'M1059', 'M1061', 'M1063', 'M1074',
        'M1075', 'M1093', 'M1168', 'M1173', 'M1174',
        'M1194', 'M1216', 'M1220', 'M1221', 'M1224',
        'M1225', 'M1228', 'M1229', 'M1237', 'M1238',
        'M1239', 'M3034')
        AND
        CurrentExpiryDate >= GETDATE()
        """,
        'directdebit_memberships': 
        """
        SELECT * FROM MembershipContractsDetails
        WHERE ProductCode IN (
        'M1076', 'M1077', 'M1083', 'M1170', 'M1209',
        'M1222', 'M1226', 'M1227', 'M1230', 'M1231',
        'M1232')
        AND
        CurrentExpiryDate >= GETDATE()
        """,
        'failed_payments': 
        """
        ...
        """,
        'all_members_initial': 
        """
        /* Need to churn through with ETLJitterbitClone functions - namely, M to Male, F to Female
        Did I mention the innerjoin function won't work in the SQL query? If it did, it
        would be enough to get the MAX CurrentExpiryDate out of the duplicate CustomerIds
        */
        SELECT
        DISTINCT MCD.CustomerId, 
        MCD.Surname, 
        MCD.GivenNames, 
        MCD.Description, 
        CONVERT(varchar, MCD.DateStarted, 23) AS DateStarted, 
        CONVERT(varchar, MCD.CurrentExpiryDate, 23) AS CurrentExpiryDate, 
        MCD.Address, 
        MCD.Suburb,
        MCD.State,
        MCD.PostCode, 
        MCD.HomePhone, 
        MCD.WorkPhone, 
        MCD.MobilePhone, 
        MCD.Email, 
        CONVERT(varchar, MCD.DateOfBirth, 23) AS DateOfBirth, 
        MCD.Gender, 
        PE.Status, 
	Profiles.DateLastUpdated AS IDLastUpdated, /* caters for profile changes e.g. address, email, gender lol etc on People table*/
	MCC.CiD AS ContractLastUpdated, /* caters for contract renewals, cancellations - any changes made to the existing membership contract is timestamped (ideally AQ staff should be updating existing contracts as opposed to creating new ones */
	MCD.CustomerDateCreated AS CustomerDateCreated /* same timestamp as DateCreated field of People table */
        FROM MembershipContractsDetails AS MCD /* main table from which other relevant data is attached */
        LEFT JOIN PeopleEblast AS PE /* required for status filter - needed unfortunately due to dirty Links data */
        ON MCD.CustomerId=PE.Id 
        LEFT JOIN (SELECT DISTINCT ContractId, MAX(DateTime) AS CiD FROM MembershipContractChanges GROUP BY ContractId) AS MCC
        ON MCD.Id=MCC.ContractId /* date when contract details were amended by AQ staff */
	LEFT JOIN People AS Profiles /* required for date of when customer profile info is changed e.g. address change etc */
	ON MCD.CustomerId=Profiles.Id
        WHERE MCD.ProductCode IN ( /* membership types - includes DD and upfront payment */
        'M1050', 'M1059', 'M1061', 'M1063', 'M1074', 
        'M1075', 'M1093', 'M1168', 'M1173', 'M1174', 
        'M1194', 'M1216', 'M1220', 'M1221', 'M1224', 
        'M1225', 'M1228', 'M1229', 'M1237', 'M1238', 
        'M1239', 'M3034', 'M1076', 'M1077', 'M1083', 
        'M1170', 'M1209', 'M1222', 'M1226', 'M1227', 
        'M1230', 'M1231', 'M1232'
	)
	AND PE.Status = 'ACTIVE'
        AND
        (CurrentExpiryDate >= GETDATE() /* expiry date has to be more than today */
        OR
        CurrentExpiryDate IS NULL
        )
        AND NOT MCD.DateStarted > GETDATE() /* otherwise there are duplicate CustomerIDs as even though there is only ever one active membership */
        """,
        'all_members_nightly': 
        """
        SELECT
        DISTINCT MCD.CustomerId, 
        MCD.Surname, 
        MCD.GivenNames, 
        MCD.Description, 
        CONVERT(varchar, MCD.DateStarted, 23) AS DateStarted, 
        CONVERT(varchar, MCD.CurrentExpiryDate, 23) AS CurrentExpiryDate, 
        MCD.Address, 
        MCD.Suburb,
        MCD.State,
        MCD.PostCode, 
        MCD.HomePhone, 
        MCD.WorkPhone, 
        MCD.MobilePhone, 
        MCD.Email, 
        CONVERT(varchar, MCD.DateOfBirth, 23) AS DateOfBirth, 
        MCD.Gender, 
        PE.Status, 
	Profiles.DateLastUpdated AS IDLastUpdated, /* caters for profile changes e.g. address, email, gender lol etc on People table*/
	MCC.CiD AS ContractLastUpdated, /* caters for contract renewals, cancellations - any changes made to the existing membership contract is timestamped (ideally AQ staff should be updating existing contracts as opposed to creating new ones */
	MCD.CustomerDateCreated AS CustomerDateCreated /* same timestamp as DateCreated field of People table */
        FROM MembershipContractsDetails AS MCD /* main table from which other relevant data is attached */
        LEFT JOIN PeopleEblast AS PE /* required for status filter - needed unfortunately due to dirty Links data */
        ON MCD.CustomerId=PE.Id 
        LEFT JOIN (SELECT DISTINCT ContractId, MAX(DateTime) AS CiD FROM MembershipContractChanges GROUP BY ContractId) AS MCC
        ON MCD.Id=MCC.ContractId /* date when contract details were amended by AQ staff */
	LEFT JOIN People AS Profiles /* required for date of when customer profile info is changed e.g. address change etc */
	ON MCD.CustomerId=Profiles.Id
        WHERE MCD.ProductCode IN ( /* membership types - includes DD and upfront payment */
        'M1050', 'M1059', 'M1061', 'M1063', 'M1074', 
        'M1075', 'M1093', 'M1168', 'M1173', 'M1174', 
        'M1194', 'M1216', 'M1220', 'M1221', 'M1224', 
        'M1225', 'M1228', 'M1229', 'M1237', 'M1238', 
        'M1239', 'M3034', 'M1076', 'M1077', 'M1083', 
        'M1170', 'M1209', 'M1222', 'M1226', 'M1227', 
        'M1230', 'M1231', 'M1232'
	)
	AND
	PE.Status = 'Active'
	AND
	(MCC.CiD >= DATEADD(d, datediff(d, 0, getdate()),-1) AND MCC.CiD < dateadd(d,datediff(d,0, getdate()),0) /* Only yesterday. Not today, if today is required, remove AND clause */
	OR 
	Profiles.DateLastUpdated >= DATEADD(d, datediff(d, 0, getdate()),-1) AND Profiles.DateLastUpdated < dateadd(d,datediff(d,0, getdate()),0) /* Only yesterday. Not today, if today is required, remove AND clause */
	OR
	MCD.DateStarted >= DATEADD(d, datediff(d, 0, getdate()),-1) AND MCD.DateStarted < dateadd(d,datediff(d,0, getdate()),0) /* Only yesterday. Not today, if today is required, remove AND clause */
	)
	AND
	(CurrentExpiryDate >= DATEADD(d, datediff(d, 0, getdate()),-1) /* final filter is only generate rows that have an expiry date more than or equal to today! */
	OR
	CurrentExpiryDate IS NULL /* direct debit memberships with no expiry */
	)
        /* Above AND and OR clauses filter a la: ContractLastUpdated yesterday | OR | IDLastUpdated yesterday | OR | DateStarted yesterday */
        /* No need for Profile last created yesterday clause as Profile creation date does not necessarily mean Health Club Membership association */
        """,
        'info': 
        """
        The most often used (the only one used) in health club (hc) dictionary is 'all_members_nightly' query.
        """         
    }

# SwimSchool
ss = {'student_bookings_weekly':
        """
        SELECT
        /* make M Male and F female */
        SBD.Surname AS StuSurname,
        SBD.GivenNames AS StuGivenNames,
        StudentId AS StudId, LessonDayDesc AS LessonDay, 
        CONVERT(VARCHAR(8), LessonTime, 108) as LessonTime,
        LessonLevel, Area, TeacherSurname AS TeachSurname, 
        TeacherGivenNames AS TeachGivenNames, CONVERT(VARCHAR,
        StartDate, 23) AS StuBookStartDate, 
        SBD.CeaseDate,
        RespSurname AS RPSurname, RespGivenNames AS RPGivenNames, 
        ResponsiblePersonId AS RPId, RespAddress AS RPAddress,
        RespSuburb AS RPSuburb, RespPostCode AS RPPostCode,
        RespHomePhone AS RPHomePhone, RespWorkPhone AS RPWorkPhone, 
        RespMobilePhone AS RPMobilePhone, RespEmail AS RPEmail,
        People.State AS RPState, SBD.Gender AS StuGender, 
        CONVERT(VARCHAR, SBD.DateOfBirth, 23) AS StuDateOfBirth,
        DATEDIFF(hour, SBD.DateOfBirth, GETDATE()) / 8766.0 as StuAge
        FROM StudentBookingDetails AS SBD
        LEFT JOIN People
        ON SBD.ResponsiblePersonId=People.Id
        WHERE
	CeaseDate IS NULL
	/* ensures that only non cancelled student bookings are returned */
	"""
      }


