select *
from {{ source('bronzelayer', 'trends') }}
