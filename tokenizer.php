<?php

function tokenize_config($filename)
{
  $tokens = token_get_all(file_get_contents($filename));
  $search_tokens = ['$table_prefix', '='];
  $stop_tokens = [';'];
  $found_tokens = [];
  $state = false;
  foreach ($tokens as $token) {
    if (is_string($token)) {
        if(array_search($token, $search_tokens) !== false) {
          array_push($found_tokens, $token);
        } else if(count($found_tokens) >= 2) {
          $state = true;
        }
        if($state && array_search($token, $stop_tokens) !== false) {
          array_push($found_tokens, $token);
          return array_values(array_filter(preg_replace('\s+', "", $found_tokens)));
        }
    } else {
        // token array
        list($id, $text) = $token;
        $token = strval($text);
        switch ($id) { 
            case T_COMMENT: 
            case T_DOC_COMMENT:
                // no action on comments
                break;

            default:
              if(array_search($token, $search_tokens) !== false) {
                array_push($found_tokens, $token);
              } else if(count($found_tokens) >= 2) {
                $state = true;
              }
              if($state && array_search($token, $stop_tokens) !== false) {
                array_push($found_tokens, $token);
                return array_values(array_filter(preg_replace('\s+', "", $found_tokens)));
              }
              break;
        }
    }
  }
  return array();
}

/**
 * Returns root directory of wordpress from a folder parameter
 */
function search_root_directory($folder)
{
  $patterns = [$folder . '/wp-config.php', $folder . '/**/wp-config.php'];
  foreach($patterns as $pattern) {
    $g  = glob($pattern);
    if(!empty($g)) {
      return dirname($g[0]);
    }
  }
}