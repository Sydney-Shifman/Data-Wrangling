import pandas as pd
import numpy as np
import nibrs_mapping

#---------------------------------------
# LOADING DATA
#---------------------------------------
print("\nBeginning processing of NY data...")
print("Beginning loading of NY data...")
# Load raw NY data
historic_df = pd.read_csv("NYPD_Arrests_Data__Historic_.csv")
present_df = pd.read_csv("NYPD_Arrest_Data__Year_to_Date_.csv")

# Rename column for continuity for merging
historic_df.rename(columns={'Lon_Lat':'Location'}, inplace=True)
print(f"\t- Renamed Lon_Lat to Location for merging")

# Merge raw data together
combined_df = pd.concat([historic_df, present_df], ignore_index=True)
print(f"\t- Merged data from .csv files together")
print(f"Finished loading NY data containing {combined_df.shape[0]} rows")
#---------------------------------------
# CREATING MAPPING FOR DATA
#---------------------------------------
print("Beginning creating mapping for NY data...")
# Normalize PD_DESC Dictionary
normalize_desc = {
    "F.O.A. NON-SUPPORT": "F.C.A. NON SUPPORT",
    "NYC UNCLASSIFIED WARRANT": "NY CITY,TRAFFIC SUMMONS WARRANT",
    "NY CITY,UNCLASSIFIED WARRANT": "NY CITY,TRAFFIC SUMMONS WARRANT",
    "FUGITIVE/OTHER JURISDICTION NYS": "FUGITIVE,FROM OTHER JURISDICTION IN NY STATE",
    "FUGITIVE/OTHER STATES": "FUGITIVE,FROM OTHER STATES",
    "NYS PAROLE VIOLATION": "NY STATE,PAROLE",
    "NYS PROBATION": "NY STATE,PROBATION",
    "NYS UNCLASSIFIED": "NY STATE,UNCLASSIFIED",
    "US CODE,UNCLASSIFIED": "U.S. CODE UNCLASSIFIED",
    "VEHICULAR ASSAULT (INTOX DRIVE": "VEHICULAR ASSAULT (INTOX DRIVER)",
    "ASSAULT POLICE/PEACE OFFICER": "ASSAULT 2,1,PEACE OFFICER",
    "END WELFARE VULNERABLE ELDERLY PERSON": "ENDANGERING VULNERABLE ELDERLY",
    "MENACING 1ST DEGREE (VICT NOT": "MENACING 1ST DEGREE (VICT NOT PEACE OFFICER)",
    "HOMICIDE, NEGLIGENT, VEHICLE,": "HOMICIDE, NEGLIGENT, VEHICLE, INTOX DRIVER",
    "HOMICIDE,NEGLIGENT,UNCLASSIFIE": "HOMICIDE,NEGLIGENT,UNCLASSIFIED",
    "MANSLAUGHTER,UNCLASSIFIED - NO": "MANSLAUGHTER,UNCLASSIFIED - NON NEGLIGENT",
    "FAC. SEXUAL OFFENSE W/CONTROLL": "FAC. SEXUAL OFFENSE W/CONTROLLED SUBSTANCE",
    "AGGRAVATED SEXUAL ASBUSE": "SEXUAL ABUSE 1",
    "COURSE OF SEXUAL CONDUCT AGAIN": "COURSE OF SEXUAL CONDUCT AGAINST A CHILD",
    "TRESPASS 4,CRIMINAL SUB 2": "TRESPASS 4,CRIMINAL",
    "BURGLARY,UNCLASSIFIED,UNKNOWN": "BURGLARY,UNCLASSIFIED,UNKNOWN TIME",
    "RADIO DEVICES,UNLAWFUL POSSESS": "RADIO DEVICES,UNLAWFUL POSSESSION",
    "MISCHIEF, CRIMINAL 4, OF MOTOR": "MISCHIEF, CRIMINAL 4, OF MOTOR VEHICLE",
    "CRIMINAL MISCHIEF 4TH, GRAFFIT": "CRIMINAL MISCHIEF 4TH, GRAFFITI",
    "MISCHIEF,CRIMINAL     UNCLASSIFIED 4TH DEG": "CRIMINAL MISCHIEF,UNCLASSIFIED 4",
    "MISCHIEF,CRIMINAL     UNCLASSI": "CRIMINAL MISCHIEF,UNCLASSIFIED 4",
    "MISCHIEF, CRIMINAL 3 & 2, OF M": "MISCHIEF, CRIMINAL 3 & 2, OF MOTOR VEHICLE",
    "MISCHIEF,CRIMINAL,    UNCL 2ND": "MISCHIEF,CRIMINAL,UNCLASSIFIED 2ND DEG 3RD DEG",
    "RECKLESS ENDANGERMENT OF PROPE": "RECKLESS ENDANGERMENT OF PROPERTY",
    "LARCENY,PETIT FROM OPEN AREAS,": "LARCENY,PETIT FROM OPEN AREAS,UNCLASSIFIED",
    "ROBBERY,CARJACKING OF MV OTHER THAN TRUCK": "ROBBERY,CAR JACKING",
    "ROBBERY,UNCLASSIFIED,OPEN AREAS": "ROBBERY,OPEN AREA UNCLASSIFIED",
    "ROBBERY,UNCLASSIFIED,OPEN AREA": "ROBBERY,OPEN AREA UNCLASSIFIED",
    "LARCENY,GRAND BY THEFT OF CREDIT CARD": "LARCENY,GRAND BY CREDIT CARD USE",
    "LARCENY,GRAND FROM PERSON,UNCL": "LARCENY,GRAND FROM PERSON,UNCLASSIFIED",
    "LARCENY,GRAND FROM BUILDING,UNCLASSIFIED" :"LARCENY,GRAND FROM BUILDING (NON-RESIDENCE) UNATTENDED",
    "LARCENY,GRAND FROM OPEN AREAS,": "LARCENY,GRAND FROM OPEN AREAS, UNATTENDED",
    "LARCENY,GRAND FROM OPEN AREAS,UNCLASSIFIED": "LARCENY,GRAND FROM OPEN AREAS, UNATTENDED",
    "THEFT OF SERVICES- CABLE TV SE": "THEFT OF SERVICES- CABLE TV SERVICE",
    "THEFT OF SERVICES, UNCLASSIFIE": "THEFT OF SERVICES, UNCLASSIFIED",
    "THEFT,RELATED OFFENSES,UNCLASS": "THEFT,RELATED OFFENSES,UNCLASSIFIED",
    "STOLEN PROPERTY-MOTOR VEH 2ND,": "STOLEN PROPERTY-MOTOR VEH 2ND, 1ST POSSESS",
    "STOLEN PROPERTY 2,1,POSSESSION": "STOLEN PROPERTY 2,1,POSSESSION,UNCLASSIFIED",
    "STOLEN PROPERTY 2,POSSESSION B": "STOLEN PROPERTY 2,POSSESSION BY LICENSED DEA",
    "CONTROLLED SUBSTANCE,POSSESS.": "CONTROLLED SUBSTANCE,POSSESS. 1",
    "CONTROLLED SUBSTANCE,INTENT TO": "CONTROLLED SUBSTANCE,INTENT TO SELL 3",
    "CONTROLLED SUBSTANCE, POSSESSI": "CONTROLLED SUBSTANCE, POSSESSION 4",
    "DRUG PARAPHERNALIA,   POSSESSE": "DRUG PARAPHERNALIA, POSSESSES OR SELLS",
    "DRUG PARAPHERNALIA,   POSSESSES OR SELLS 1": "DRUG PARAPHERNALIA, POSSESSES OR SELLS",
    "DRUG PARAPHERNALIA,   POSSESSES OR SELLS 2": "DRUG PARAPHERNALIA, POSSESSES OR SELLS",
    "POSSESSION HYPODERMIC INSTRUME": "POSSESSION HYPODERMIC INSTRUMENT",
    "CONTROLLED SUBSTANCE, INTENT T": "CONTROLLED SUBSTANCE, INTENT TO SELL 5",
    "GAMBLING 2, PROMOTING, BOOKMAK": "GAMBLING 2, PROMOTING, BOOKMAKING",
    "GAMBLING 2, PROMOTING, POLICY-": "GAMBLING 2, PROMOTING, POLICY-LOTTERY",
    "GAMBLING 2,PROMOTING,UNCLASSIF": "GAMBLING 2,PROMOTING,UNCLASSIFIED",
    "GAMBLING 1,PROMOTING,BOOKMAKIN": "GAMBLING 1,PROMOTING,BOOKMAKING",
    "PROSTITUTION 4,PROMOTING&SECUR": "PROSTITUTION 4,PROMOTING&SECURING",
    "PROSTITUTION 3, PROMOTING UNDE": "PROSTITUTION 3, PROMOTING UNDER 19",
    "PROSTITUTION 3,PROMOTING BUSIN": "PROSTITUTION 3,PROMOTING BUSINESS",
    "UNLAWFUL POSS. WEAPON UPON SCH": "UNLAWFUL POSS. WEAPON UPON SCHOOL GROUNDS",
    "OBSCENE MATERIAL - UNDER 17 YE": "OBSCENE MATERIAL - UNDER 17 YEARS OF AGE",
    "LOITERING 1ST DEGREE FOR DRUG": "LOITERING 1ST DEGREE FOR DRUG PURPOSES",
    "LOITERING,TRANSPORTATION FACIL":"LOITERING,TRANSPORTATION FACILITY",
    "LOITERING FOR PROSTITUTION OR": "LOITERING FOR PROSTITUTION OR TO PATRONIZE",
    "UNDER THE INFLUENCE, DRUGS": "UNDER THE INFLUENCE OF DRUGS",
    "DISORDERLY CONDUCT SUBD 1,2,3,4,5,6,7": "DISORDERLY CONDUCT",
    "FALSE ALARM FIRE": "FALSE REPORT 1,FIRE",
    "NUISANCE, CRIMINAL": "NUISANCE,CRIMINAL,UNCLASSIFIED",
    "MATERIAL              OFFENSIV": "MATERIAL OFFENSIVE DISPLAY",
    "SUPP. ACT TERR 2ND": "SUPP. ACT TERRORISM 2ND",
    "TERRORISM PROVIDE SUPPORT": "SUPP ACT TERRORISM 1",
    "TERRORIST THREAT": "MAKING TERRORISTIC THREAT",
    "PRIVACY,OFFENSES AGAINST,UNCLA": "PRIVACY,OFFENSES AGAINST,UNCLASSIFIED",
    "INCOMPETENT PERSON,RECKLESSY ENDANGERING": "INCOMPETENT PERSON,ENDANGERING WELFARE",
    "INCOMPETENT PERSON,KNOWINGLY ENDANGERING": "INCOMPETENT PERSON,ENDANGERING WELFARE",
    "PROMOTING A SEXUAL PERFORMANCE": "PROMOTING A SEXUAL PERFORMANCE BY A CHILD",
    "USE OF A CHILD IN A SEXUAL PER": "USE OF A CHILD IN A SEXUAL PERFORMANCE",
    "IMPERSONATION 2, PUBLIC SERVAN": "IMPERSONATION 2, PUBLIC SERVANT",
    "IMPERSONATION 1, POLICE OFFICE": "IMPERSONATION 1, POLICE OFFICER",
    "FRAUD,UNCLASSIFIED-MISDEMEANOR,PART 1": "FRAUD,UNCLASSIFIED-MISDEMEANOR",
    "FRAUD,UNCLASSIFIED-MISDEMEANOR-PART 2": "FRAUD,UNCLASSIFIED-MISDEMEANOR",
    "FORGERY-ILLEGAL POSSESSION,VEH": "FORGERY-ILLEGAL POSSESSION,VEHICLE IDENT. NU",
    "FORGERY,ETC.,UNCLASSIFIED-FELO": "FORGERY,ETC.,UNCLASSIFIED-FELONY",
    "MANUFACTURE UNAUTHORIZED RECOR": "MANUFACTURE UNAUTHORIZED RECORDINGS",
    "SALE OF UNAUTHORIZED RECORDING": "SALE OF UNAUTHORIZED RECORDINGS",
    "IDENTITY THFT-2": "IDENTITY THFT-1",
    "FRAUD,UNCLASSIFIED-FELONY": "IDENTITY THFT-1",
    "APPEARANCE TICKET FAIL TO RESP": "APPEARANCE TICKET FAIL TO RESPOND",
    "PUBLIC ADMINISTATION,UNCLASS M": "PUBLIC ADMINISTATION,UNCLASS MISDEMEAN 4",
    "PUBLIC ADMINISTRATION,UNCLASSI": "PUBLIC ADMINISTRATION,UNCLASSIFIED FELONY",
    "CRIMINAL DISPOSAL FIREARM 1": "CRIMINAL DISPOSAL FIREARM 1 & 2",
    "CRIMINAL DISPOSAL FIREARM 1 &": "CRIMINAL DISPOSAL FIREARM 1 & 2",
    "WEAPONS DISPOSITION OF": "WEAPONS,DISPOSITION OF",
    "WEAPONS,PROHIBITED USE": "WEAPONS,PROHIBITED USE IMITATION PISTOL",
    "WEAPONS,PROHIBITED USE IMITATI": "WEAPONS,PROHIBITED USE IMITATION PISTOL",
    "UNFINSH FRAME 2": "WEAPONS,PROHIBITED USE IMITATION PISTOL",
    "CRIMINAL POSSESSION WEAPON": "WEAPONS POSSESSION 1 & 2",
    "WOUNDS,REPORTING OF": "CRIM POS WEAP 4",
    "LICENSING FIREARMS": "FIREARMS LICENSING LAWS",
    "PUBLIC SAFETY,UNCLASSIFIED MIS": "PUBLIC SAFETY,UNCLASSIFIED MISDEMEANOR",
    "ALCOHOLIC BEVERAGE CONTROL": "ALCOHOLIC BEVERAGE CONTROL LAW",
    "AGRICULTURE & MARKETS LAW,UNCL": "AGRICULTURE & MARKETS LAW,UNCLASSIFIED",
    "GENERAL BUSINESS LAW / UNCLASSIFIED": "GENERAL BUSINESS LAW,UNCLASSIFIED",
    "PUBLIC HEALTH LAW,UNCLASSIFIED": "PUBLIC HEALTH LAW,UNCLASSIFIED MISDEMEANOR",
    "NY STATE LAWS,UNCLASSIFIED FEL": "NY STATE LAWS,UNCLASSIFIED FELONY",
    "NY STATE LAWS,UNCLASSIFIED MIS": "NY STATE LAWS,UNCLASSIFIED MISDEMEANOR",
    "NY STATE LAWS,UNCLASSIFIED VIO": "NY STATE LAWS,UNCLASSIFIED VIOLATION",
    "ADM.CODE,UNCLASSIFIED VIOLATIO": "ADM.CODE,UNCLASSIFIED VIOLATION",
    "INTOXICATED DRIVING,ALCOHOL": "INTOXICATED DRIVING,ALCOHOL",
    "IMPAIRED DRIVING,DRUG": "IMPAIRED DRIVING, DRUGS",
    "IMPAIRED DRIVING / ALCOHOL": "IMPAIRED DRIVING,ALCOHOL",
    "LEAVING SCENE-ACCIDENT-PERSONA": "LEAVING SCENE-ACCIDENT-PERSONAL-INJURY",
    "TRAFFIC,UNCLASSIFIED MISDEMEAN": "TRAFFIC,UNCLASSIFIED MISDEMEANOR",
    "BICYCLE(TRAF.INFRAC. UNCLASS.)": "BICYCLE TRAFFIC INFRACTION UNCLASSIFIED",
    "FOLLOWING CLOSELY": "FOLLOWING TOO CLOSELY",
    "LIGHTS,IMPROPER": "IMPROPER LIGHTS",
    "LEAVING SCENE-ACCIDENT-PROP. DAMAGE": "LEAVING THE SCENE / PROPERTY DAMAGE / INJURED ANIMAL",
    "ONE WAY STREET": "ONE-WAY STREET",
    "PEDESTRIAN - WALK/DO NOT WALK": "PEDESTRIAN,WALK/DONT WALK",
    "PEDESTRIAN / UNCLASSIDIED": "PEDESTRIAN,UNCLASSIFIED",
    "ROGHT OF WAY / VEHICLE": "RIGHT OF WAY,VEHICLE",
    "SIGNAL,FAIL TO": "FAIL TO STOP ON SIGNAL",
    "STOP,FAIL TO,ON SIGNAL": "FAIL TO STOP ON SIGNAL",
    "FAIL TO SIGNAL": "FAIL TO STOP ON SIGNAL",
    "TRAFFIC,UNCLASSIFIED INFRACTIO": "TRAFFIC,UNCLASSIFIED INFRACTION",
    "SAFETY BELTS": "SEAT BELTS",
    "USE OF CELLULAR TELEPHONE WHILE DRIVING": "USE CELL PHONE WHILE DRIVING",
    "PARKR&R,UNCLASSIFIED VIOLATION": "PARKING,UNCLASSIFIED",
    "UNCLASSIFIED": "PARKING,UNCLASSIFIED",
    "CHILD,OFFENSES AGAINST,UNCLASS": "CHILD,OFFENSES AGAINST,UNCLASSIFIED"
}
print(f"\t- Created Normalized PD_DESC Dictionary")

# PD_DESC to PD_CD Dictionary
desc_to_pdcd = {
    "F.C.A. NON SUPPORT": "1",
    "F.C.A. ORDER OF PROTECTION": "2",
    "F.C.A. P.I.N.O.S.": "4",
    "F.C.A. UNCLASSIFIED": "9",
    "NY CITY,TRAFFIC SUMMONS WARRANT": "12",
    "FUGITIVE,FROM OTHER JURISDICTION IN NY STATE": "15",
    "FUGITIVE,FROM OTHER STATES": "16",
    "NY STATE,PAROLE": "29",
    "NY STATE,PROBATION": "30",
    "NY STATE,UNCLASSIFIED": "35",
    "U.S. CODE UNCLASSIFIED": "49",
    "STALKING COMMIT SEX OFFENSE": "100",
    "ASSAULT 3": "101",
    "VEHICULAR ASSAULT (INTOX DRIVER)": "104",
    "STRANGULATION 1ST": "105",
    "ASSAULT 2,1,PEACE OFFICER": "106",
    "ENDANGERING VULNERABLE ELDERLY": "107",
    "ASSAULT OTHER PUBLIC SERVICE EMPLOYEE": "108",
    "ASSAULT 2,1,UNCLASSIFIED": "109",
    "MENACING 1ST DEGREE (VICT NOT PEACE OFFICER)": "112",
    "MENACING,UNCLASSIFIED": "113",
    "OBSTR BREATH/CIRCUL": "114",
    "RECKLESS ENDANGERMENT 2": "115",
    "RECKLESS ENDANGERMENT 1": "117",
    "PROMOTING SUICIDE ATTEMPT": "119",
    "HOMICIDE, NEGLIGENT, VEHICLE, INTOX DRIVER": "122",
    "HOMICIDE,NEGLIGENT,UNCLASSIFIED": "125",
    "MANSLAUGHTER,UNCLASSIFIED - NON NEGLIGENT": "129",
    "MURDER,UNCLASSIFIED": "139",
    "ABORTION 1": "143",
    "ABORTION 2, 1, SELF": "146",
    "RAPE 3": "153",
    "RAPE 2": "155",
    "RAPE 1": "157",
    "SODOMY 3": "164",
    "SODOMY 2": "166",
    "SODOMY 1": "168",
    "SEXUAL MISCONDUCT,INTERCOURSE": "170",
    "FORCIBLE TOUCHING": "173",
    "SEXUAL MISCONDUCT,DEVIATE": "174",
    "SEXUAL ABUSE 3,2": "175",
    "SEX CRIMES": "176",
    "SEXUAL ABUSE": "177",
    "FAC. SEXUAL OFFENSE W/CONTROLLED SUBSTANCE": "178",
    "SEXUAL ABUSE 1": "179",
    "COURSE OF SEXUAL CONDUCT AGAINST A CHILD": "180",
    "IMPRISONMENT 2,UNLAWFUL": "181",
    "IMPRISONMENT 1,UNLAWFUL": "183",
    "LABOR TRAFFICKING": "184",
    "KIDNAPPING 2": "185",
    "LURING A CHILD": "186",
    "KIDNAPPING 1": "187",
    "CUSTODIAL INTERFERENCE 2": "191",
    "CUSTODIAL INTERFERENCE 1": "193",
    "COERCION 2": "195",
    "COERCION 1": "197",
    "CRIMINAL CONTEMPT 1": "198",
    "AGGRAVATED CRIMINAL CONTEMPT": "199",
    "TRESPASS 4,CRIMINAL": "201",
    "TRESPASS 3, CRIMINAL": "203",
    "TRESPASS 1,CRIMINAL": "204",
    "TRESPASS 2, CRIMINAL": "205",
    "BURGLARS TOOLS,UNCLASSIFIED": "209",
    "BURGLARY,COMMERCIAL,DAY": "211",
    "BURGLARY,RESIDENCE,NIGHT": "223",
    "BURGLARY,UNCLASSIFIED,UNKNOWN TIME": "244",
    "RADIO DEVICES,UNLAWFUL POSSESSION": "248",
    "MISCHIEF, CRIMINAL 4, OF MOTOR VEHICLE": "254",
    "MISCHIEF, CRIMINAL 4, BY FIRE": "256",
    "CRIMINAL MISCHIEF 4TH, GRAFFITI": "258",
    "CRIMINAL MISCHIEF,UNCLASSIFIED 4": "259",
    "ARSON 1": "261",
    "ARSON 2,3,4": "263",
    "MISCHIEF 1,CRIMINAL,EXPLOSIVE": "265",
    "MISCHIEF, CRIMINAL 3 & 2, OF MOTOR VEHICLE": "267",
    "CRIMINAL MIS 2 & 3": "268",
    "MISCHIEF,CRIMINAL,UNCLASSIFIED 2ND DEG 3RD DEG": "269",
    "TAMPERING 3,2, CRIMINAL": "271",
    "TAMPERING 1,CRIMINAL": "273",
    "POSTING ADVERTISEMENTS": "275",
    "RECKLESS ENDANGERMENT OF PROPERTY": "277",
    "SOLICITATION 5,CRIMINAL": "281",
    "SOLICITATION 4, CRIMINAL": "283",
    "SOLICITATION 3,2,1, CRIMINAL": "285",
    "CONSPIRACY 6, 5": "289",
    "CONSPIRACY 4, 3": "291",
    "CONSPIRACY 2, 1": "293",
    "FACILITATION 4, CRIMINAL": "297",
    "FACILITATION 3,2,1, CRIMINAL": "299",
    "LARCENY,PETIT BY ACQUIRING LOS": "301",
    "LARCENY,PETIT FROM OPEN AREAS,UNCLASSIFIED": "339",
    "ROBBERY,GAS STATION": "379",
    "ROBBERY,CAR JACKING": "380",
    "ROBBERY,OPEN AREA UNCLASSIFIED": "397",
    "LARCENY,GRAND BY ACQUIRING LOS": "401",
    "LARCENY,GRAND BY CREDIT CARD USE": "405",
    "LARCENY,GRAND BY EXTORTION": "409",
    "LARCENY,GRAND FROM PERSON,UNCLASSIFIED": "419",
    "LARCENY,GRAND BY IDENTITY THEFT-UNCLASSIFIED": "432",
    "LARCENY,GRAND FROM BUILDING (NON-RESIDENCE) UNATTENDED": "438",
    "LARCENY,GRAND FROM OPEN AREAS, UNATTENDED": "439",
    "AGGRAVATED GRAND LARCENY OF ATM": "440",
    "LARCENY,GRAND OF AUTO": "441",
    "UNAUTHORIZED USE VEHICLE 2": "461",
    "UNAUTHORIZED USE VEHICLE 3": "462",
    "JOSTLING": "464",
    "ACCOSTING,FRAUDULENT": "466",
    "FORTUNE TELLING": "468",
    "UNAUTH. SALE OF TRANS. SERVICE": "475",
    "CREDIT CARD,UNLAWFUL USE OF": "476",
    "THEFT OF SERVICES- CABLE TV SERVICE": "477",
    "THEFT OF SERVICES, UNCLASSIFIED": "478",
    "THEFT,RELATED OFFENSES,UNCLASSIFIED": "479",
    "STOLEN PROPERTY 3,POSSESSION": "490",
    "STOLEN PROPERTY-MOTOR VEH 2ND, 1ST POSSESS": "493",
    "STOLEN PROPERTY 2,1,POSSESSION,UNCLASSIFIED": "494",
    "STOLEN PROPERTY 2,POSSESSION BY LICENSED DEA": "498",
    "CONTROLLED SUBSTANCE,POSSESS. 1": "500",
    "CONTROLLED SUBSTANCE,POSSESS. 2": "501",
    "CONTROLLED SUBSTANCE,POSSESS. 3": "502",
    "CONTROLLED SUBSTANCE,INTENT TO SELL 3": "503",
    "CONTROLLED SUBSTANCE, POSSESSION 4": "505",
    "CONTROLLED SUBSTANCE, POSSESSION 5": "507",
    "DRUG PARAPHERNALIA, POSSESSES OR SELLS": "508",
    "POSSESSION HYPODERMIC INSTRUMENT": "509",
    "CONTROLLED SUBSTANCE, INTENT TO SELL 5": "510",
    "CONTROLLED SUBSTANCE, POSSESSION 7": "511",
    "CONTROLLED SUBSTANCE,SALE 1": "512",
    "POSS METH MANUFACT MATERIAL": "513",
    "CONTROLLED SUBSTANCE,SALE 2": "514",
    "CONTROLLED SUBSTANCE,SALE 3": "515",
    "SALE SCHOOL GROUNDS 4": "519",
    "CONTROLLED SUBSTANCE, SALE 4": "520",
    "CONTROLLED SUBSTANCE, SALE 5": "521",
    "USE CHILD TO COMMIT CONT SUB OFF": "522",
    "SALE SCHOOL GROUNDS": "523",
    "SALES OF PRESCRIPTION": "529",
    "DRUG, INJECTION OF": "530",
    "CONTROLLED SUBSTANCE,POSSESS. OF PROCURSERS": "532",
    "GAMBLING 2, PROMOTING, BOOKMAKING": "533",
    "GAMBLING 2, PROMOTING, POLICY-LOTTERY": "537",
    "GAMBLING 2,PROMOTING,UNCLASSIFIED": "544",
    "GAMBLING, DEVICE, POSSESSION": "548",
    "GAMBLING 1,PROMOTING,BOOKMAKING": "553",
    "GAMBLING 1,PROMOTING,POLICY": "557",
    "PROSTITUTION, PATRONIZING 2, 1": "561",
    "PROSTITUTION": "563",
    "PROSTITUTION, PATRONIZING 4, 3": "565",
    "MARIJUANA, POSSESSION": "566",
    "MARIJUANA, POSSESSION 4 & 5": "567",
    "MARIJUANA, POSSESSION 1, 2 & 3": "568",
    "MARIJUANA, SALE 4 & 5": "569",
    "MARIJUANA, SALE 1, 2 & 3": "570",
    "PROSTITUTION,PERMITTING": "574",
    "PROSTITUTION 4,PROMOTING&SECURING": "576",
    "CANNABIS POSSESSION": "577",
    "CANNABIS POSSESSION, 3": "578",
    "CANNABIS POSSESSION, 2&1": "579",
    "CANNABIS SALE": "580",
    "CANNABIS SALE, 3": "581",
    "CANNABIS SALE, 2&1": "582",
    "CANNABIS SALE, AGGRAVATED": "583",
    "PROSTITUTION 3, PROMOTING UNDER 19": "584",
    "PROSTITUTION 3,PROMOTING BUSINESS": "585",
    "SEX TRAFFICKING": "586",
    "PROSTITUTION 2, COMPULSORY": "587",
    "PROSTITUTION 2, UNDER 16": "588",
    "PROSTITUTION 1, UNDER 11": "589",
    "OBSCENITY, PERFORMANCE 3": "591",
    "OBSCENITY, MATERIAL 3": "593",
    "OBSCENITY 1": "594",
    "UNLAWFUL POSS. WEAPON UPON SCHOOL GROUNDS": "595",
    "OBSCENE MATERIAL - UNDER 17 YEARS OF AGE": "596",
    "LOITERING,BEGGING": "600",
    "LOITERING 1ST DEGREE FOR DRUG PURPOSES": "604",
    "LOITERING,GAMBLING,OTHER": "610",
    "LOITERING,MASQUERADING": "612",
    "LOITERING,SCHOOL": "614",
    "LOITERING,TRANSPORTATION FACILITY": "616",
    "LOITERING FOR PROSTITUTION OR TO PATRONIZE": "618",
    "UNDER THE INFLUENCE OF DRUGS": "622",
    "DISORDERLY CONDUCT": "625",
    "DIS. CON.,AGGRAVATED": "627",
    "EXHIBITION,OFFENSIVE": "631",
    "EXPOSURE OF A PERSON": "633",
    "HARASSMENT,SUBD 1,CIVILIAN": "637",
    "HARASSMENT,SUBD 3,4,5": "638",
    "AGGRAVATED HARASSMENT 2": "639",
    "AGGRAVATED HARASSMENT 1": "640",
    "HEALTHCARE/RENT.REG.": "641",
    "ASSEMBLY,UNLAWFUL": "643",
    "FALSE REPORT 1,FIRE": "644",
    "FALSE REPORT BOMB": "647",
    "PLACE FALSE BOMB": "648",
    "FALSE REPORT UNCLASSIFIED": "649",
    "RIOT 2/INCITING": "652",
    "NUISANCE,CRIMINAL,UNCLASSIFIED": "659",
    "LEWDNESS,PUBLIC": "661",
    "MATERIAL OFFENSIVE DISPLAY": "662",
    "SUPP. ACT TERRORISM 2ND": "663",
    "SUPP ACT TERRORISM 1": "664",
    "MAKING TERRORISTIC THREAT": "665",
    "HIND PROSEC. TERR 2": "667",
    "PRIVACY,OFFENSES AGAINST,UNCLASSIFIED": "669",
    "RIOT 1": "672",
    "EAVESDROPPING": "674",
    "ANARCHY,CRIMINAL": "678",
    "CHILD, ENDANGERING WELFARE": "681",
    "CHILD,LICENSED PREMISES": "685",
    "CHILD,OFFENSES AGAINST,UNCLASSIFIED": "687",
    "INCOMPETENT PERSON,ENDANGERING WELFARE": "688",
    "MARRIAGE,OFFENSES AGAINST,UNCLASSIFIED": "689",
    "BIGAMY": "691",
    "INCEST": "693",
    "INCEST 3": "694",
    "CHILD ABANDONMENT": "695",
    "PROMOTING A SEXUAL PERFORMANCE BY A CHILD": "696",
    "USE OF A CHILD IN A SEXUAL PERFORMANCE": "697",
    "BRIBERY,COMMERCIAL": "701",
    "CHECK,BAD": "703",
    "FORGERY,ETC.-MISD.": "705",
    "RECORDS,FALSIFY-TAMPER": "706",
    "IMPERSONATION 2, PUBLIC SERVANT": "707",
    "IMPERSONATION 1, POLICE OFFICER": "708",
    "TAMPERING WITH A WITNESS": "711",
    "USING SLUGS, 2ND": "713",
    "POSSESSION ANTI-SECURITY ITEM": "715",
    "FRAUD,UNCLASSIFIED-MISDEMEANOR": "718",
    "BRIBERY,FRAUD": "721",
    "FORGERY-ILLEGAL POSSESSION,VEHICLE IDENT. NU": "724",
    "FORGERY,M.V. REGISTRATION": "725",
    "FORGERY,PRESCRIPTION": "727",
    "FORGERY,ETC.,UNCLASSIFIED-FELONY": "729",
    "MANUFACTURE UNAUTHORIZED RECORDINGS": "730",
    "SALE OF UNAUTHORIZED RECORDINGS": "731",
    "ENTERPRISE CORRUPTION": "733",
    "USURY,CRIMINAL": "737",
    "IDENTITY THFT-1": "739",
    "ESCAPE 3": "742",
    "BAIL JUMPING 3": "744",
    "PERJURY 3,ETC.": "746",
    "CONTEMPT,CRIMINAL": "748",
    "VIOLATION OF ORDER OF PROTECTI": "749",
    "RESISTING ARREST": "750",
    "APPEARANCE TICKET FAIL TO RESPOND": "754",
    "ABSCONDING FROM WORK RELEASE 2": "756",
    "PUBLIC ADMINISTATION,UNCLASS MISDEMEAN 4": "759",
    "BRIBERY,PUBLIC ADMINISTRATION": "760",
    "ESCAPE 2,1": "762",
    "BAIL JUMPING 1 & 2": "764",
    "PERJURY 2,1,ETC": "766",
    "COMPUTER UNAUTH. USE/TAMPER": "770",
    "MONEY LAUNDERING 3": "771",
    "COMPUTER TAMPER/TRESSPASS": "772",
    "MONEY LAUNDERING 1 & 2": "775",
    "PUBLIC ADMINISTRATION,UNCLASSIFIED FELONY": "779",
    "CRIMINAL DISPOSAL FIREARM 1 & 2": "781",
    "WEAPONS, POSSESSION, ETC": "782",
    "FIREWORKS PREV CONV 5 YEARS": "783",
    "WEAPONS,MFR,TRANSPORT,ETC.": "784",
    "WEAPONS,DISPOSITION OF": "785",
    "WEAPONS,PROHIBITED USE IMITATION PISTOL": "786",
    "FIREWORKS, SALE": "788",
    "FIREWORKS, POSSESS/USE": "789",
    "WEAPONS POSSESSION 1 & 2": "792",
    "WEAPONS POSSESSION 3": "793",
    "MANUFACTURE, TRANSPORT, DEFACE, ETC...": "794",
    "CRIM POS WEAP 4": "797",
    "FIREARMS LICENSING LAWS": "798",
    "PUBLIC SAFETY,UNCLASSIFIED MISDEMEANOR": "799",
    "ALCOHOLIC BEVERAGE CONTROL LAW": "801",
    "A.B.C.,FALSE PROOF OF AGE": "803",
    "TAX LAW": "808",
    "AGRICULTURE & MARKETS LAW,UNCLASSIFIED": "812",
    "NEGLECT/POISON ANIMAL": "815",
    "TORTURE/INJURE ANIMAL CRUELTY": "817",
    "ABANDON ANIMAL": "818",
    "GENERAL BUSINESS LAW,UNCLASSIFIED": "827",
    "NAVIGATION LAW": "836",
    "PUBLIC HEALTH LAW,UNCLASSIFIED MISDEMEANOR": "841",
    "INAPPROPIATE SHELTER DOG LEFT": "843",
    "CAUSE SPI/KILL ANIMAL": "844",
    "CONFINING ANIMAL IN VEHICLE/SHELTER": "846",
    "NY STATE LAWS,UNCLASSIFIED FELONY": "847",
    "NY STATE LAWS,UNCLASSIFIED MISDEMEANOR": "848",
    "NY STATE LAWS,UNCLASSIFIED VIOLATION": "849",
    "ALCOHOLIC BEVERAGES,PUBLIC CON": "862",
    "NOISE,UNECESSARY": "872",
    "PEDDLING,UNLAWFUL": "874",
    "UNLAWFUL SALE SYNTHETIC MARIJUANA": "876",
    "UNLAWFUL DISCLOSURE OF AN INTIMATE IMAGE": "877",
    "ADM.CODE,UNCLASSIFIED MISDEMEA": "878",
    "ADM.CODE,UNCLASSIFIED VIOLATION": "879",
    "HEALTH CODE,VIOLATION": "889",
    "INTOXICATED DRIVING,ALCOHOL": "905",
    "IMPAIRED DRIVING, DRUGS": "906",
    "IMPAIRED DRIVING,ALCOHOL": "909",
    "LEAVING SCENE-ACCIDENT-PERSONAL-INJURY": "916",
    "RECKLESS DRIVING": "918",
    "TRAFFIC,UNCLASSIFIED MISDEMEANOR": "922",
    "BICYCLE TRAFFIC INFRACTION UNCLASSIFIED": "932",
    "FOLLOWING TOO CLOSELY": "933",
    "IMPROPER LIGHTS": "939",
    "LEAVING THE SCENE / PROPERTY DAMAGE / INJURED ANIMAL": "940",
    "ONE-WAY STREET": "943",
    "IMPROPER PASSING": "944",
    "PEDESTRIAN,WALK/DONT WALK": "945",
    "PEDESTRIAN,UNCLASSIFIED": "947",
    "RIGHT OF WAY,PEDESTRIAN": "955",
    "RIGHT OF WAY,VEHICLE": "957",
    "FAIL TO STOP ON SIGNAL": "961",
    "SPEEDING": "963",
    "TURN,IMPROPER": "967",
    "UNLICENSED OPERATOR": "968",
    "TRAFFIC,UNCLASSIFIED INFRACTION": "969",
    "SPILLBACK": "970",
    "SEAT BELTS": "972",
    "USE CELL PHONE WHILE DRIVING": "973",
    "PARKING,UNCLASSIFIED": "997"
}
print(f"\t- Created PD_DESC to PD_CD Dictionary")

# PD_CD to NIBRS_CD Dictionary
pdcd_to_nibrs = {
    # Kidnapping
    "193": "100","191": "100","183": "100","181": "100","187": "100","185": "100",
    "186": "100",

    # Robbery
    "380": "120","379": "120", "397": "120",

    # Arson
    "261": "200","263": "200","265": "200","256": "200","663": "200",

    # Extortion/Blackmail
    "733": "210","409": "210",

    # Burglary/Breaking & Entering
    "209": "220","211": "220","223": "220","244": "220", "204": "220","205": "220",
    "203": "220","201": "220",

    # Motor Vehicle Theft
    "461": "240","462": "240",

    # Counterfeiting/Forgery
    "803": "250","724": "250","705": "250","729": "250","725": "250","727": "250",
    "730": "250","766": "250","746": "250","706": "250","731": "250","475": "250",
    "713": "250",

    # Stolen Property Offenses
    "494": "280","498": "280","490": "280","493": "280",

    # Destruction/Damage/Vandalism of Property
    "268": "290","258": "290","259": "290","267": "290","254": "290","269": "290",
    "275": "290","277": "290","273": "290","271": "290",

    # Pornography/Obscene Material
    "631": "370","633": "370","661": "370","662": "370","596": "370","594": "370",
    "593": "370","591": "370","877": "370","697": "370",

    # Violation of No Contact Orders
    "199": "500","2": "500","749": "500",

    # Bribery
    "701": "510","721": "510", "760": "510",

    # Weapon Law Violations
    "797": "520","781": "520","792": "520","798": "520","794": "520","595": "520",
    "785": "520","793": "520","782": "520","784": "520","786": "520",

    # Animal Cruelty
    "818": "720","844": "720","846": "720","815": "720","817": "720",

    # Not Reportable to NIBRS
    "961": "999","933": "999","15": "999","16": "999","939": "999","944": "999",
    "29": "999","30": "999","943": "999","997": "999","947": "999","945": "999",
    "955": "999","957": "999","972": "999","963": "999","970": "999","969": "999",
    "922": "999","967": "999","973": "999",

    # Murder & Nonnegligent Manslaughter
    "129": "09A","139": "09A","285": "09A","664": "09A",

    # Negligent Manslaughter
    "122": "09B","125": "09B",

    # Rape
    "179": "11A","178": "11A","157": "11A","155": "11A","153": "11A","176": "11A",
    "170": "11A",

    # Sodomy
    "174": "11B","168": "11B","166": "11B","164": "11B",

    # Fondling
    "173": "11D","177": "11D","175": "11D",

    # Aggravated Assault
    "106": "13A","109": "13A","108": "13A","114": "13A","119": "13A","117": "13A",
    "105": "13A","104": "13A",

    # Simple Assault
    "101": "13B","627": "13B","107": "13B","115": "13B","283": "13B",

    # Intimidation
    "640": "13C","639": "13C","197": "13C","195": "13C","665": "13C","112": "13C",
    "113": "13C","648": "13C","100": "13C","711": "13C",

    # Shoplifting
    "464": "23C",

    # Theft From Building
    "440": "23D","438": "23D","439": "23D","339": "23D",

    # Theft From Motor Vehicle
    "441": "23F",

    # All Other Larceny
    "401": "23H","419": "23H","301": "23H","477": "23H","478": "23H","479": "23H",

    # False Pretenses/Swindle/Confidence Game
    "466": "26A","468": "26A","718": "26A","808": "26A","737": "26A",

    # Credit Card/Automated Teller Machine Fraud
    "405": "26B",

    # Impersonation
    "476": "26C","708": "26C","707": "26C",

    # Identity Theft
    "739": "26F","432": "26F",

    # Hacking/Computer Invasion
    "772": "26G","770": "26G",

    # Drug/Narcotic Violations
    "577": "35A","579": "35A","578": "35A","580": "35A","582": "35A","581": "35A","583": "35A",
    "510": "35A","505": "35A","507": "35A","511": "35A","520": "35A","521": "35A","503": "35A",
    "500": "35A","501": "35A","502": "35A","532": "35A","512": "35A","514": "35A","515": "35A",
    "530": "35A","566": "35A","568": "35A","567": "35A","570": "35A","569": "35A","513": "35A",
    "523": "35A","519": "35A","529": "35A","876": "35A","522": "35A",

    # Drug Equipment Violations
    "508": "35B","509": "35B",

    # Incest
    "693": "36A","694": "36A",

    # Statutory Rape
    "180": "36B",

    # Betting/Wagering
    "553": "39A","557": "39A","533": "39A","537": "39A","544": "39A",

    # Gambling Equipment Violation
    "548": "39C",

    # Prostitution
    "589": "40A","587": "40A","588": "40A", "563": "40A",

    # Assisting or Promoting Prostitution
    "696": "40B","584": "40B","585": "40B","576":"40B","574": "40B",

    # Purchasing Prostitution
    "618": "40C","561": "40C","565": "40C",

    # Human Trafficking, Commercial Sex Acts
    "586": "64A",

    # Human Trafficking, Involuntary Servitude
    "184": "64B",

    # Bad Checks
    "703": "90A",

    # Curfew/Loitering/Vagrancy Violations
    "604": "90B","600": "90B","610": "90B","612": "90B","614": "90B","616": "90B",

    # Disorderly Conduct
    "643": "90C","625": "90C","872": "90C","672": "90C","652": "90C",

    # Driving Under the Influence
    "909": "90D","906": "90D","905": "90D","918": "90D","622": "90D",

    # Family Offenses, Nonviolent
    "695": "90F","681": "90F","687": "90F","1": "90F","9": "90F","688": "90F",

    # Liquor Law Violations
    "801": "90G","862": "90G",

    # Peeping Tom
    "669": "90H",

    # Runaway
    "4": "90I",

    # All Other Offenses
    "143": "90Z","146": "90Z","756": "90Z","878": "90Z","879": "90Z","812": "90Z","678": "90Z",
    "754": "90Z","764": "90Z","744": "90Z","932": "90Z","691": "90Z","685": "90Z","293": "90Z",
    "291": "90Z","289": "90Z","748": "90Z","198": "90Z","674": "90Z","762": "90Z","742": "90Z",
    "299": "90Z","297": "90Z","644": "90Z","647": "90Z","649": "90Z","783": "90Z","789": "90Z",
    "788": "90Z","827": "90Z","637": "90Z","638": "90Z","889": "90Z","641": "90Z","667": "90Z",
    "843": "90Z","916": "90Z","940": "90Z","689": "90Z","775": "90Z","771": "90Z","836": "90Z",
    "659": "90Z","847": "90Z","848": "90Z","849": "90Z","35": "90Z","12": "90Z","874": "90Z",
    "759": "90Z","779": "90Z","841": "90Z","799": "90Z","248": "90Z","750": "90Z","281": "90Z",
    "49": "90Z","968": "90Z"
}
print(f"\t- Created PD_CD to NIBRS_CD Dictionary")

# Borough Ltr to Borough Name Dictionary
ltr_to_borough = {
    "B": "The Bronx", "K": "Brooklyn", "M": "Manhattan", "Q": "Queens", "S": "Staten Island"
}
print(f"\t- Created Boro_Ltr to Boro_Name Dictionary")
print("Finished creating mapping for data")
#---------------------------------------
# PROCESSING MAPPING
#---------------------------------------
print("Beginning mapping of data...")
# Create a copy for clean version
clean_combined_df = combined_df.copy()

# Convert PD_CD to Int64Dtype (handles null values) then string to match mapping dictionary
clean_combined_df['PD_CD'] = clean_combined_df['PD_CD'].astype(pd.Int64Dtype()).astype(str)

# Normalize the PD_DESC values based on the normalization dictionary
clean_combined_df['PD_DESC_NORM'] = clean_combined_df['PD_DESC'].map(normalize_desc).fillna(clean_combined_df['PD_DESC'])
print(f"\t- Mapped PD_DESC_NORM")

# Map each normalized PD_DESC to its PD_CD
clean_combined_df['PD_CD_NORM'] = clean_combined_df['PD_DESC_NORM'].map(desc_to_pdcd)
print(f"\t- Mapped PD_CD_NORM")

# Map each PD_CD to its NIBRS_offense_code
clean_combined_df['NIBRS_offense_code'] = clean_combined_df['PD_CD_NORM'].map(pdcd_to_nibrs)
print(f"\t- Mapped NIBRS_offense_code")

# Map each NIBRS_offense_code to its NIBRS Offense Code Description
clean_combined_df['NIBRS Offense Code Description'] = clean_combined_df['NIBRS_offense_code'].map(nibrs_mapping.nibrs_to_desc)
print(f"\t- Mapped NIBRS Offense Code Description")

# Map each NIBRS Offense Code Description to its Offense Category
clean_combined_df['Offense Category'] = clean_combined_df['NIBRS Offense Code Description'].map(nibrs_mapping.nibrs_desc_to_cat)
print(f"\t- Mapped Offense Category")

clean_combined_df['Borough'] = clean_combined_df['ARREST_BORO'].map(ltr_to_borough)
print(f"\t- Mapped Borough Name")
print("Finished mapping for data")
#---------------------------------------
# CLEANING MAPPED OUT DATA
#---------------------------------------
print("Beginning cleaning of data...")
# Make null values across dataset into np.nan
print(f"\t- Converted {clean_combined_df.isin(['null', '(null)']).sum().sum()} null/unknown values to np.nan")
clean_combined_df.replace(['null', '(null)'], np.nan, inplace=True)

# Remove rows that don't have a PD_DESC as no way to map
clean_combined_df = clean_combined_df[clean_combined_df['PD_DESC'].notnull()].copy()
print(f"\t- Removed {combined_df.shape[0] - clean_combined_df.shape[0]} rows that represent an unidentified report")

print("Finished cleaning data")
#---------------------------------------
# ADDING HOSPITAL LOCATIONS TO DATA
#---------------------------------------
print("Beginning adding nearest hospital locations to data...")
# Load cleaned hospital data for use
clean_hospital_df = pd.read_csv('clean_hospital.csv')

# Prepare hospital arrays
hosp_lats = clean_hospital_df['Latitude'].to_numpy(dtype=float)
hosp_lons = clean_hospital_df['Longitude'].to_numpy(dtype=float)
hosp_names = clean_hospital_df['Hospital Name'].to_numpy(dtype=object)
hosp_addrs = clean_hospital_df['Address'].to_numpy(dtype=object)

# Prepare result columns with default np.nan
nearest_names = np.full(len(clean_combined_df), np.nan, dtype=object)
nearest_addrs = np.full(len(clean_combined_df), np.nan, dtype=object)

# Mask of rows with valid coordinates (non-null and not NaN)
valid_mask = clean_combined_df['Latitude'].notna() & clean_combined_df['Longitude'].notna()

if valid_mask.any():
    crime_lats = clean_combined_df.loc[valid_mask, 'Latitude'].to_numpy(dtype=float)
    crime_lons = clean_combined_df.loc[valid_mask, 'Longitude'].to_numpy(dtype=float)

    # Compute squared Euclidean distances in a vectorized way:
    # shape -> (n_crimes, n_hospitals)
    dlat = crime_lats[:, None] - hosp_lats[None, :]
    dlon = crime_lons[:, None] - hosp_lons[None, :]
    d2 = dlat * dlat + dlon * dlon

    # For each crime, find index of nearest hospital
    nearest_idx = np.argmin(d2, axis=1)

    # Map back to names and addresses
    nearest_names[valid_mask.to_numpy()] = hosp_names[nearest_idx]
    nearest_addrs[valid_mask.to_numpy()] = hosp_addrs[nearest_idx]

# Assign to two separate columns
clean_combined_df['Nearest Hospital'] = nearest_names
clean_combined_df['Hospital Address'] = nearest_addrs
print(f"\t- Added column to identify nearest hospital")
print(f"\t- Added column to identify hospital address")
print("Finished adding nearest hospital locations to data")
#---------------------------------------
# FILTER OUT CLEANED DATA FOR COMBINING
#---------------------------------------
print("Beginning filtering data...")
# Drop columns not shared by datasets
num_columns = len(clean_combined_df.columns)
clean_combined_df.drop(columns=['ARREST_BORO', 'PD_CD', 'PD_DESC', 'KY_CD', 'OFNS_DESC', 'LAW_CODE', 'LAW_CAT_CD', 'JURISDICTION_CODE', 'AGE_GROUP', 'PERP_SEX',
                                'PERP_RACE', 'X_COORD_CD', 'Y_COORD_CD', 'Location', 'PD_DESC_NORM', 'PD_CD_NORM'], inplace=True)
print(f"\t- Dropped {num_columns - len(clean_combined_df.columns)} columns that aren't shared between data")

# Rename columns for continuity between datasets
clean_combined_df.rename(columns={'ARREST_KEY': 'Report Number'}, inplace=True)
clean_combined_df.rename(columns={'ARREST_DATE': 'Report Date'}, inplace=True)
clean_combined_df.rename(columns={'ARREST_PRECINCT': 'Reported Location'}, inplace=True)
clean_combined_df.rename(columns={'Borough': 'Reported Area'}, inplace=True)
clean_combined_df.rename(columns={'Offense Category': 'NIBRS Category'}, inplace=True)
clean_combined_df.rename(columns={'NIBRS Offense Code Description': 'NIBRS Desc'}, inplace=True)
clean_combined_df.rename(columns={'NIBRS_offense_code': 'NIBRS Code'}, inplace=True)
print(f"\t- Renamed columns for continuity when combining data")

# Create column to identify city of crime
clean_combined_df['City'] = 'New York'
print(f"\t- Added column to identify city of crime")

# Reorder the columns
clean_combined_df = clean_combined_df[['City', 'Report Number', 'Report Date', 'NIBRS Code', 'NIBRS Desc', 'NIBRS Category', 'Reported Area',
                                       'Reported Location', 'Latitude', 'Longitude', 'Nearest Hospital', 'Hospital Address']]
print(f"\t- Reordered columns to be more organized when combining data")
print("Finished filtering data")
print("Finished processing of NY data")
#---------------------------------------
# EXPORTING CLEANED DATA
#---------------------------------------
print(f"Beginning exporting NY data containing {clean_combined_df.shape[0]} rows...")
# Create .csv file for cleaned version of data
clean_combined_df.to_csv('clean_ny.csv', index=False)
print("Finished exporting cleaned and filtered data to clean_ny.csv\n")
