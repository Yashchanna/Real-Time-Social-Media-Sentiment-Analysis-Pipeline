select *
from {{ source('bronzelayer', 'valid_tweets') }}
