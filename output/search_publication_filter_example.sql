-- Search query pattern with channel publication status
-- Replace :channel_code, :publication_status, :search with your app parameters.

SELECT
    p.id,
    p.source_product_key,
    p.source_model_no,
    p.brand,
    p.manufacturer,
    p.product_title_en,
    p.category_en,
    sc.code AS channel_code,
    COALESCE(pub.publication_status, 'NOT_PUBLISHED') AS publication_status,
    COALESCE(pub.is_published, false) AS is_published,
    pub.external_product_id,
    pub.published_at,
    pub.last_sync_at
FROM probuy.source_products p
JOIN probuy.product_search_documents psd
  ON psd.source_product_id = p.id
LEFT JOIN probuy.sales_channels sc
  ON sc.code = :channel_code
LEFT JOIN probuy.product_channel_publications pub
  ON pub.source_product_id = p.id
 AND pub.channel_id = sc.id
WHERE p.is_active = true
  AND (:publication_status IS NULL OR COALESCE(pub.publication_status, 'NOT_PUBLISHED') = :publication_status)
  AND (:is_published IS NULL OR COALESCE(pub.is_published, false) = :is_published)
  AND psd.search_vector @@ plainto_tsquery('english', :search)
ORDER BY p.product_title_en NULLS LAST
LIMIT 50;
