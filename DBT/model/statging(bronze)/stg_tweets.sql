select *
from {{ source('bronzelayer', 'tweets') }}
