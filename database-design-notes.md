# ProBuy probuy-product-intelligence — Database Design Notes

## 1. Purpose
This document defines the relational database schema for the PriceSense platform.

The database must:
- Support multiple product data sources (starting with SCN International)
- Store structured product, pricing, inventory, and attribute data
- Preserve source traceability using JSONB
- Support strong search and filtering
- Handle large datasets efficiently

## 2. Core Concepts
### Primary Source
A supplier dataset (e.g., SCN International)

### Source Product
A product as defined by a specific source

### Attribute
A flexible key-value property (e.g., color, size, material)

### Location
Warehouse or pricing region (e.g., VAN, MTL, EDM)

## 3. Schema Namespace
All tables must be under:
probuy

## 4. Tables

### primary_sources
id UUID PRIMARY KEY
code TEXT UNIQUE NOT NULL
name TEXT NOT NULL
is_active BOOLEAN DEFAULT TRUE
created_at TIMESTAMPTZ DEFAULT now()

### import_batches
id UUID PRIMARY KEY
source_id UUID
import_type TEXT
file_name TEXT
imported_at TIMESTAMPTZ DEFAULT now()
row_count INTEGER
status TEXT
metadata JSONB

### source_products
id UUID PRIMARY KEY
source_id UUID
source_product_key TEXT
source_model_no TEXT
brand TEXT
manufacturer TEXT
product_title_en TEXT
description_en TEXT
category_en TEXT
unit_description_en TEXT
raw_data JSONB
created_at TIMESTAMPTZ DEFAULT now()

UNIQUE(source_id, source_product_key)

### source_locations
id UUID PRIMARY KEY
source_id UUID
code TEXT
name TEXT
province TEXT
country TEXT DEFAULT 'CA'

### source_product_prices
id UUID PRIMARY KEY
source_product_id UUID
location_id UUID
model_no TEXT
list_price NUMERIC(12,2)
distributor_cost NUMERIC(12,2)
pricing_update_date TIMESTAMPTZ
raw_data JSONB

### source_product_inventory
id UUID PRIMARY KEY
source_product_id UUID
location_id UUID
model_no TEXT
stock_status TEXT
quantity_available NUMERIC
inventory_update_date TIMESTAMPTZ
raw_data JSONB

### product_images
id UUID PRIMARY KEY
source_product_id UUID
image_position INTEGER
image_url TEXT
is_primary BOOLEAN DEFAULT FALSE
raw_data JSONB

### attribute_definitions
id UUID PRIMARY KEY
canonical_name TEXT
display_name TEXT
data_type TEXT
unit TEXT

### product_attribute_values
id UUID PRIMARY KEY
source_product_id UUID
attribute_id UUID
value_text TEXT
value_numeric NUMERIC
unit TEXT
raw_data JSONB

### product_search_documents
source_product_id UUID PRIMARY KEY
search_text TEXT
search_vector TSVECTOR
brand TEXT
category TEXT
attributes JSONB

## 5. Relationships
primary_sources → source_products
source_products → prices, inventory, attributes, images, search

## 6. Example
Product: 3M Blade

Attributes:
- length = 3
- unit = inch
- color = black

Price:
- VAN = 12.99

Inventory:
- VAN = 120
