<?php

class MatchQuery
{
    /**
     * 
     */
    public static function sql_builder_match_all($wpdb, $titles, $post_type, $post_status = "", $score_min = 10, $score_max = 0, $tag_separator = ".")
    {
        if(empty($titles))
            throw new Exception("Titles missing, please pass as array()");

        $query = "SELECT pt.ID AS ID1, pt.post_title AS Title1, LENGTH(pt.post_content) AS Description_length1, 
        GROUP_CONCAT(DISTINCT CONCAT_WS('{$tag_separator}', terms.name, taxonomy.taxonomy) ORDER BY taxonomy.taxonomy SEPARATOR '|')
        AS Terms1";
        $field_query = [];
        $where_query = [];
        $idx = 0;
        $queryAppend = "pt.post_type = '{$post_type}'";
        if(!empty($post_status)) {
            $queryAppend .= " AND pt.post_status = '{$post_status}'";
        }
        foreach($titles as $title) {
            $q = "(MATCH pt.post_title AGAINST ('{$title}' IN NATURAL LANGUAGE MODE) >= {$score_min}";
            if(!empty($score_max)) {
                $q .= " AND MATCH pt.post_title AGAINST ('{$title}' IN NATURAL LANGUAGE MODE) <= {$score_max})";
            }
            array_push($field_query, "MATCH pt.post_title AGAINST ('{$title}' IN NATURAL LANGUAGE MODE) AS {$idx}");
            array_push($where_query, $q);
            $idx++;
        }
        $query .= "," . implode(",", $field_query) . " FROM `{$wpdb->prefix}posts` AS pt 
        INNER JOIN `{$wpdb->prefix}term_relationships` AS rel ON rel.object_id = pt.ID
        INNER JOIN `{$wpdb->prefix}term_taxonomy` AS taxonomy ON taxonomy.term_taxonomy_id = rel.term_taxonomy_id
        INNER JOIN `{$wpdb->prefix}terms` AS terms ON terms.term_id = rel.term_taxonomy_id
        WHERE {$queryAppend}" . implode(" OR ", $where_query) . " GROUP BY pt.ID";
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
            WHERE {$queryAppend}" . implode(" OR ", $where_query) . " AND pt2.ID = '{$id}' GROUP BY pt.ID, pt2.ID");
        }
        return implode(' UNION ', $query);
    }
}