select *
from {{ source('bronzelayer', 'user_metadata') }}
