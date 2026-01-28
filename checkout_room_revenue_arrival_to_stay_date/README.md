this Redshift SQL script performs the following

gets the min and max extract timestamps from checkout detail for the current version of the reservation
* there can be many versions of reservation so picking current version is important
date logic, filter arrival and departure date for evaluation period

gets reservation details for all non-cancelled reservations for all hotels for current version of the reservation
date logic, filter arrival and departure date for evaluation period

determine the stay days before and after evaluation period to exclude
need to include same day arrivals and departures in filter conditions

get room nights from booking data

calculate stay_period_inclusion_factor
this is the inclusion ratio that represents portion of the stay dates during a stay that are inside evaluation period

checkout detail and checkout transaction tables are not joined directly due to large size of data in each table
checkout transaction queries are filtered using the min and max extract timestamps captured earlier to limit the data

from checkout transactions, calculate the primary charge category at the (reservation, folio, and transaction) level

get checkout transaction details for room charge_category
folio = transaction folio
add column for primary charge category

perform revenue logic calculations
3 iterative options
sum all results

the stay_period_inclusion_factor is applied to revenue amounts to calculate the portion of revenue to include in evaluation period

provide currency conversion to show USD revenue amounts


