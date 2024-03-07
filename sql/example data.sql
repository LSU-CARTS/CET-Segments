select * 
from RefRoadwayData
where ControlSection = '817-40' and Year = 2022 and substring(LRSID,7,3) = '-1-'
order by BeginLogMile

select StateCaseNumber
, CrashDate
, MannerCollision
, CrashSeverity
, c.RoadwayDeparture
, c.Pedestrian
, c.Bicycle
, c.IsIntersection
, c.JunctionLocation
, c.LogMile
from FactCrash c join RefRoadwayData r on c.HWYSectionsAK = r.HWYSectionsSK
where CrashYear between 2018 and 2022
and c.ControlSection = '817-40'
and substring(r.LRSID,7,3) in ('-1-','-2-')
