<?php

class MatchQuery
{
    public static function sql_builder($wpdb, $titles, $score_min = 10, $score_max = 30)
    {
        if(empty($titles))
            throw new Exception("Titles missing, please pass as array()");

        $query = "SELECT pt.ID, pt.post_title";
        $field_query = [];
        $where_query = [];
        $idx = 0;
        foreach($titles as $title) {
            array_push($field_query, "MATCH pt.post_title AGAINST ('{$title}' IN NATURAL LANGUAGE MODE) AS {$idx}");
            array_push($where_query, "(MATCH pt.post_title AGAINST ('{$title}' IN NATURAL LANGUAGE MODE) >= {$score_min} AND 
            MATCH pt.post_title AGAINST ('{$title}' IN NATURAL LANGUAGE MODE) <= {$score_max})");
            $idx++;
        }
        $query .= "," . implode(",", $field_query) . " FROM `{$wpdb->prefix}posts` AS pt WHERE " . implode(" OR ", $where_query);
        return $query;
    }
}