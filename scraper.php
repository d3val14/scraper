<?php

ini_set('memory_limit', '1024M');
set_time_limit(0);

/* ================= ENV ================= */

define('CURR_URL', rtrim(getenv('CURR_URL'), '/'));
define('SITEMAP_OFFSET', (int)(getenv('SITEMAP_OFFSET') ?: 0));
define('MAX_SITEMAPS', (int)(getenv('MAX_SITEMAPS') ?: 0));
define('MAX_URLS_PER_SITEMAP', (int)(getenv('MAX_URLS_PER_SITEMAP') ?: 0));

define('SITEMAP_INDEX', CURR_URL . '/sitemap.xml');
define('OUTPUT_CSV', 'products_chunk_' . SITEMAP_OFFSET . '.csv');

/* ================= HTTP ================= */

function httpGet(string $url): ?string
{
    for ($i = 0; $i < 3; $i++) {
        $data = @file_get_contents($url, false, stream_context_create([
            'http' => [
                'timeout' => 30,
                'user_agent' => 'Parallel-Scraper/1.0'
            ]
        ]));
        if ($data !== false) return $data;
        usleep(300000);
    }
    return null;
}

function loadXml(string $url): ?SimpleXMLElement
{
    $xml = httpGet($url);
    return $xml ? simplexml_load_string($xml) : null;
}

function fetchJson(string $url): ?array
{
    $json = httpGet($url);
    return $json ? json_decode($json, true) : null;
}

function normalizeImage(string $url): string
{
    return str_starts_with($url, '//') ? 'https:' . $url : $url;
}

/* ================= PRODUCT ================= */

function processProduct(string $url, $csv, array &$seen): void
{
    if (isset($seen[$url])) return;
    $seen[$url] = true;

    $product = fetchJson(rtrim($url, '/') . '.js');
    if (!$product || empty($product['variants'])) return;

    $options = $product['options'] ?? [];
    $images  = implode(',', array_map('normalizeImage', $product['images'] ?? []));

    foreach ($product['variants'] as $v) {
        fputcsv($csv, [
            $product['id'],
            trim($product['title']),
            trim($product['vendor']),
            trim($product['type']),
            trim($product['handle']),
            $v['id'],
            trim($v['title']),
            $v['sku'] ?? '',
            $options[0]['name'] ?? '',
            $v['option1'] ?? '',
            $options[1]['name'] ?? '',
            $v['option2'] ?? '',
            $options[2]['name'] ?? '',
            $v['option3'] ?? '',
            $v['price'],
            $v['available'] ? '1' : '0',
            rtrim($url, '/') . '?variant=' . $v['id'],
            $images
        ]);
    }

    usleep(150000);
}

/* ================= MAIN ================= */

$index = loadXml(SITEMAP_INDEX);
if (!$index) exit(1);

$index->registerXPathNamespace('ns', 'http://www.sitemaps.org/schemas/sitemap/0.9');
$sitemaps = $index->xpath('//ns:sitemap/ns:loc') ?: [];

$sitemaps = array_slice(
    $sitemaps,
    SITEMAP_OFFSET,
    MAX_SITEMAPS > 0 ? MAX_SITEMAPS : null
);

$csv = fopen(OUTPUT_CSV, 'w');

/* ---- CLEAN HEADER (ONLY DATA COLUMNS) ---- */
fputcsv($csv, [
    'product_id','product_title','vendor','type','handle',
    'variant_id','variant_title','sku',
    'option_1_name','option_1_value',
    'option_2_name','option_2_value',
    'option_3_name','option_3_value',
    'variant_price','available','variant_url','image_url'
]);

$seen = [];

foreach ($sitemaps as $map) {
    $xml = loadXml((string)$map);
    if (!$xml) continue;

    $ns = $xml->getNamespaces(true);
    $xml->registerXPathNamespace('ns', $ns[''] ?? '');

    $urls = $xml->xpath('//ns:url/ns:loc') ?: [];

    if (MAX_URLS_PER_SITEMAP > 0) {
        $urls = array_slice($urls, 0, MAX_URLS_PER_SITEMAP);
    }

    foreach ($urls as $loc) {
        processProduct((string)$loc, $csv, $seen);
    }

    unset($xml);
    gc_collect_cycles();
}

fclose($csv);