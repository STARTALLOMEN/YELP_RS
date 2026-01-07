# API Contract (Version 1.1 - Enhanced Filtering)

## Base URL
`http://localhost:8081/api/v1`

---

## 1. Health Check
`GET /api/v1/health`

### Response
```json
{"status": "ok"}
```

---

## 2. Options Endpoint
`GET /api/v1/options`

### Response
```json
{
  "locations": ["Santa Barbara, CA", "Los Angeles, CA"],
  "categories": ["Mexican", "Italian", "Chinese"]
}
```

---

## 3. Recommendation Endpoint (Enhanced)
`POST /api/v1/recommendations`

### Request
```json
{
  "location": "Santa Barbara, CA",
  "category": "Mexican",
  "filters": {
    "min_stars": 4.0,
    "max_price_range": 2,
    "has_parking": true,
    "has_wifi": true,
    "has_outdoor_seating": false,
    "is_good_for_kids": true,
    "has_delivery": false,
    "has_takeout": true
  },
  "limit": 10
}
```

### Available Filters
| Filter | Type | Description |
|--------|------|-------------|
| `min_stars` | float (1-5) | Minimum star rating |
| `max_price_range` | int (1-4) | Maximum price ($=1, $$$$=4) |
| `has_parking` | bool | Has parking available |
| `has_wifi` | bool | Has WiFi |
| `has_outdoor_seating` | bool | Has outdoor seating |
| `is_good_for_kids` | bool | Family friendly |
| `has_delivery` | bool | Offers delivery |
| `has_takeout` | bool | Offers takeout |

### Response
```json
{
  "status": "success",
  "input": {
    "location": "Santa Barbara, CA",
    "category": "Mexican",
    "filters": {"has_parking": true},
    "limit": 10
  },
  "total_found": 25,
  "suggestions": [
    {
      "business_id": "abc123",
      "business_name": "Taco Palace",
      "categories": ["Mexican", "Tacos"],
      "stars": 4.5,
      "review_count": 120,
      "location": "Santa Barbara, CA",
      "has_parking": true,
      "has_wifi": true,
      "price_range": 2,
      "has_outdoor_seating": true,
      "is_good_for_kids": true,
      "has_delivery": false,
      "has_takeout": true
    }
  ]
}
```

---

## 4. Business Insights (Planned)
`GET /api/v1/business/{business_id}/insights`

### Response (Planned)
```json
{
  "business_id": "abc123",
  "name": "Taco Palace",
  "sentiment_summary": {
    "positive_ratio": 0.85,
    "top_keywords": ["tasty", "fast service"]
  },
  "recent_vibe": "Positive but crowded"
}
```
