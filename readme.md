# homehuntr

A project to decide on apartment listings based on a set of criteria. Rankings are based on a weighted average of the criteria.

## Criteria

1. Price
2. Location (travel time to common destinations, grocery stores, etc.)
3. Amenities (laundry, dishwasher, etc.)

## Data Sources

### Google maps API

- Travel time by transportation type (train, bus, bike, walk)
- Nearby grocery stores and their ratings

### Streeteasy

Price and amenities

## ELT

```mermaid
  graph LR;
      subgraph get_data
      user_input--streeteasy url-->main.py;
      main.py--streeteasy url-->scrape_streeteasy.py;
      scrape_streeteasy.py--streeteasy.com-->apt_json[(data/address/uid.json)];
      apt_json-->get_directions;
      create_destination.py-.->destinations_json;
      destinations_json[(data/directions/destinations.json)]-->get_directions;
      get_directions--maps_api-->directions_json[(data/directions/origin_destination.json)];
      end;
      subgraph clean_data
      directions_json-->compute_distance.py;
      compute_distance.py-->delta/transit_directions;
      compute_distance.py-->dedupe_directions;
      dedupe_directions-->delta/transit_directions;
      end;
```

# TODOS

- [ ] transition all writes to gcp
