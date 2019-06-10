<?php

namespace DuplicateChecks;

class MatchQuery
{
    const WEBSITE_ATTR = "website";
    
    /**
     * 
     */
    public static function sql_builder_match_all($wpdb, $titles, $meta_fields, $post_type, $post_status = "", $score_min = 10, $score_max = 0, $tag_separator = ".")
    {
        if(empty($titles) || empty($meta_fields))
            throw new Exception("Titles missing, please pass as array()");

        $query = "SELECT pt.ID AS ID1, pt.post_title AS Title1, LENGTH(pt.post_content) AS Description_length1, 
        GROUP_CONCAT(DISTINCT CONCAT_WS('{$tag_separator}', terms.name, taxonomy.taxonomy) ORDER BY taxonomy.taxonomy SEPARATOR '|')
        AS Terms1";
        $field_query = [];
        $where_query = [];
        $on1_query = [];
        $meta_field1_append = [];
        $idx = 0;
        $queryAppend = "pt.post_type = '{$post_type}'";
        if(!empty($post_status)) {
            $queryAppend .= " AND pt.post_status = '{$post_status}'";
        }
        foreach($meta_fields as $meta_field) {
            array_push($on1_query, "(ptmeta1.meta_key = '{$meta_field}' AND ptmeta1.post_id = pt1.ID)");
            array_push($meta_field1_append, "IF(ptmeta1.meta_key = '{$meta_field}', TRIM(ptmeta1.meta_value), '') AS {$meta_field}_1");
        }
        $on1_query = implode(' OR ', $on1_query);

        $meta_field1_append = implode(',', $meta_field1_append);
        foreach($titles as $title) {
            $q = "(MATCH pt.post_title AGAINST ('{$title}' IN NATURAL LANGUAGE MODE) >= {$score_min}";
            if(!empty($score_max)) {
                $q .= " AND MATCH pt.post_title AGAINST ('{$title}' IN NATURAL LANGUAGE MODE) <= {$score_max})";
            }
            array_push($field_query, "MATCH pt.post_title AGAINST ('{$title}' IN NATURAL LANGUAGE MODE) AS {$idx}");
            array_push($where_query, $q);
            $idx++;
        }
        $query .= ", {$meta_field1_append}, " .
        implode(",", $field_query) . 
        " FROM `{$wpdb->prefix}posts` AS pt 
        LEFT JOIN `{$wpdb->prefix}postmeta` AS ptmeta1 ON ({$on1_query})
        INNER JOIN `{$wpdb->prefix}term_relationships` AS rel ON rel.object_id = pt.ID
        INNER JOIN `{$wpdb->prefix}term_taxonomy` AS taxonomy ON taxonomy.term_taxonomy_id = rel.term_taxonomy_id
        INNER JOIN `{$wpdb->prefix}terms` AS terms ON terms.term_id = rel.term_taxonomy_id
        WHERE {$queryAppend} AND (" . implode(" OR ", $where_query) . ") GROUP BY pt.ID";
        return $query;
    }

    /**
     * 
     */
    public static function sql_builder_match_entry($wpdb, $ids, $titles, $post_type, $post_status = "", $score_min = 10, $score_max = 0, $tag_separator = ".")
    {
        if(empty($titles) || empty($ids))
            throw new Exception("IDs / Titles missing, please pass as array()");

        $field_query = [];
        $where_query = [];
        $idx = 0;
        $query = [];
        $queryAppend = "pt1.post_type = '{$post_type}'";
        if(!empty($post_status)) {
            $queryAppend .= " AND pt1.post_status = '{$post_status}'";
        }
        $posts = array_combine($ids, $titles);
        foreach($posts as $id => $title) {
            $q = "(MATCH pt1.post_title AGAINST ('{$title}' IN NATURAL LANGUAGE MODE) >= {$score_min}";
            if(!empty($score_max)) {
                $q .= " AND MATCH pt1.post_title AGAINST ('{$title}' IN NATURAL LANGUAGE MODE) <= {$score_max})";
            }
            array_push($field_query, "MATCH pt1.post_title AGAINST ('{$title}' IN NATURAL LANGUAGE MODE) AS {$idx}");
            array_push($where_query, $q);
            $idx++;
        }
        foreach($posts as $id => $title) {
            array_push($query, "SELECT pt1.ID AS ID1, pt1.post_title AS Title1, pt2.ID as ID2, pt2.post_title AS Title2, 
            LENGTH(pt1.post_content) AS Description_length1, LENGTH(pt2.post_content) AS Description_length2, 
            GROUP_CONCAT(DISTINCT CONCAT_WS('{$tag_separator}', terms1.name, taxonomy1.taxonomy) ORDER BY taxonomy1.taxonomy SEPARATOR '|')
            AS Terms1, 
            GROUP_CONCAT(DISTINCT CONCAT_WS('{$tag_separator}', terms2.name, taxonomy2.taxonomy) ORDER BY taxonomy1.taxonomy SEPARATOR '|')
            AS Terms2, " . 
            implode(",", $field_query) . " FROM `{$wpdb->prefix}posts` AS pt1 
            INNER JOIN `{$wpdb->prefix}term_relationships` AS rel1 ON rel1.object_id = pt1.ID
            INNER JOIN `{$wpdb->prefix}term_taxonomy` AS taxonomy1 ON taxonomy1.term_taxonomy_id = rel1.term_taxonomy_id
            INNER JOIN `{$wpdb->prefix}terms` AS terms1 ON terms1.term_id = rel1.term_taxonomy_id
            INNER JOIN `{$wpdb->prefix}posts` AS pt2 ON (pt2.ID != pt1.ID AND pt2.post_type = '{$post_type}')
            INNER JOIN `{$wpdb->prefix}term_relationships` AS rel2 ON rel2.object_id = pt2.ID
            INNER JOIN `{$wpdb->prefix}term_taxonomy` AS taxonomy2 ON taxonomy2.trem_taxonomy_id = rel2.term_taxonomy_id
            INNER JOIN `{$wpdb->prefix}terms` AS terms2 ON terms2.term_id = rel2.term_taxonomy_id
            WHERE {$queryAppend} AND (" . implode(" OR ", $where_query) . ") AND pt2.ID = '{$id}' GROUP BY pt.ID, pt2.ID");
        }
        return implode(' UNION ', $query);
    }

    /**
     * $attributes = ['post_title', 'website']
     * $meta_fields = ['address', 'website']
     */
    public static function sql_builder_match_attribute($wpdb, $attributes, $meta_fields, $post_type, $post_status = "", $score_min = 10, $score_max = 0, $tag_separator = ".")
    {
        if(empty($attributes) || empty($ids))
            throw new Exception("Attributes missing, please pass as array()");

        $where_query = [];
        $on1_query = [];
        $on2_query = [];
        $meta_field1_append = [];
        $meta_field2_append = [];
        $idx = 0;
        $query = [];
        $meta_attributes = array_intersect($attributes, $meta_fields);
        $queryAppend = "pt1.post_type = '{$post_type}'";
        if(!empty($post_status)) {
            $queryAppend .= " AND pt1.post_status = '{$post_status}'";
        }
        $posts = array_combine($ids, $titles);
        foreach($attributes as $attribute) {
            array_push($where_query, "TRIM(pt1.`{$attribute}`) = TRIM(pt2.`{$attribute}`)");
        }
        foreach($meta_attributes as $meta_attribute) {
            array_push($where_query, "IF(ptmeta1.meta_key = '{$meta_attribute}', TRIM(ptmeta1.meta_value), 'undefined_1') = 
            IF(ptmeta2.meta_key = '{$meta_attribute}', TRIM(ptmeta2.meta_value), 'undefined_2')");
        }
        foreach($meta_fields as $meta_field) {
            array_push($on1_query, "(ptmeta1.meta_key = '{$meta_field}' AND ptmeta1.post_id = pt1.ID)");
            array_push($on2_query, "(ptmeta2.meta_key = '{$meta_field}' AND ptmeta2.post_id = pt2.ID)");
            array_push($meta_field1_append, "IF(ptmeta1.meta_key = '{$meta_field}', TRIM(ptmeta1.meta_value), '') AS {$meta_field}_1");
            array_push($meta_field2_append, "IF(ptmeta2.meta_key = '{$meta_field}', TRIM(ptmeta2.meta_value), '') AS {$meta_field}_2");
        }
        $on1_query = implode(' OR ', $on1_query);
        $on2_query = implode(' OR ', $on2_query);

        $meta_field1_append = implode(',', $meta_field1_append);
        $meta_field2_append = implode(',', $meta_field2_append);
        
        $query = "SELECT pt1.ID AS ID1, pt1.post_title AS Title1, pt2.ID as ID2, pt2.post_title AS Title2, 
        LENGTH(pt1.post_content) AS Description_length1, LENGTH(pt2.post_content) AS Description_length2, 
        GROUP_CONCAT(DISTINCT CONCAT_WS('{$tag_separator}', terms1.name, taxonomy1.taxonomy) ORDER BY taxonomy1.taxonomy SEPARATOR '|')
        AS Terms1, 
        GROUP_CONCAT(DISTINCT CONCAT_WS('{$tag_separator}', terms2.name, taxonomy2.taxonomy) ORDER BY taxonomy1.taxonomy SEPARATOR '|')
        AS Terms2, 
        {$meta_field1_append}, {$meta_field2_append}, 
        FROM `{$wpdb->prefix}posts` AS pt1 
        LEFT JOIN `{$wpdb->prefix}postmeta` AS ptmeta1 ON ({$on1_query})
        INNER JOIN `{$wpdb->prefix}term_relationships` AS rel1 ON rel1.object_id = pt1.ID
        INNER JOIN `{$wpdb->prefix}term_taxonomy` AS taxonomy1 ON taxonomy1.term_taxonomy_id = rel1.term_taxonomy_id
        INNER JOIN `{$wpdb->prefix}terms` AS terms1 ON terms1.term_id = rel1.term_taxonomy_id
        INNER JOIN `{$wpdb->prefix}posts` AS pt2 ON (pt2.ID != pt1.ID AND pt2.post_type = '{$post_type}')
        LEFT JOIN `{$wpdb->prefix}postmeta` AS ptmeta2 ON ({$on2_query})
        INNER JOIN `{$wpdb->prefix}term_relationships` AS rel2 ON rel2.object_id = pt2.ID
        INNER JOIN `{$wpdb->prefix}term_taxonomy` AS taxonomy2 ON taxonomy2.trem_taxonomy_id = rel2.term_taxonomy_id
        INNER JOIN `{$wpdb->prefix}terms` AS terms2 ON terms2.term_id = rel2.term_taxonomy_id
        WHERE {$queryAppend} AND (" . implode(" OR ", $where_query) . ") GROUP BY pt.ID, pt2.ID";
        
        return $query;
    }
}