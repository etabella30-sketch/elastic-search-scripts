select 
  bd."nBundledetailid"      as bdid,
  b."nBundleid"             as bid,
  s."nSectionid"            as sid,
  c."nCaseid"               as cid,
  bd."ZnBundledetailid"     as o_bdid,
  b."ZnBundleid"            as o_bid,
  s."ZnSectionid"           as o_sid,
  c."ZnCaseid"              as o_cid
from "BundleDetail" bd
left join "BundleMaster"  b on b."nBundleid"  = bd."nBundleid"
join "SectionMaster"      s on s."nSectionid" = bd."nSectionid"
join "CaseMaster"         c on c."nCaseid"    = s."nCaseid"
where c."ZnCaseid" = {zncaseid};
