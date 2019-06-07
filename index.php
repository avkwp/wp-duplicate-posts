<?php

define('ROOT', dirname(__FILE__));
define('SITES_PATH', dirname(dirname(ROOT))); # backoffice

print("setting the uid to `root`");
var_dump(posix_seteuid(16483));

require_once 'vendor/autoload.php';
require_once 'database.php';
require_once 'process.php';

$query = filter_input(INPUT_POST, 'query');
$site = filter_input(INPUT_POST, 'site');
if(empty($site)) {
    $site = filter_input(INPUT_POST, 'sites');
}
$prefix = filter_input(INPUT_POST, 'prefix');

if(!empty($prefix)) {
    $wpdb = new stdClass();
    $wpdb->prefix = $prefix;
    require_once 'query-builder.php';
    
}

require_once 'tokenizer.php';

require_once 'header.php';
require_once 'content.php';
require_once 'footer.php';