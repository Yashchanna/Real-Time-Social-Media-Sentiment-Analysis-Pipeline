select *
from {{ source('bronzelayer', 'sentiments') }}
