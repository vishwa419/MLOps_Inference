"""
Feast Entity Definitions
Entities are the primary keys for features (user, movie)
"""

from feast import Entity, ValueType

# User entity
user = Entity(
    name="user",
    value_type=ValueType.INT64,
    description="User ID",
    join_keys=["user_idx"]
)

# Movie entity
movie = Entity(
    name="movie",
    value_type=ValueType.INT64,
    description="Movie ID",
    join_keys=["movie_idx"]
)
