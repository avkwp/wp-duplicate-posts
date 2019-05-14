<?php 

function database_properties($filename)
{
  return parse_ini_file($filename);
}

function create_connection($dbname)
{
  $config = database_properties("database.ini");
  $dsn = "mysql:host={$config['host']};dbname={$dbname}";
  $db = new PDO($dsn, $config['username'], $config['password']);
  return $db;
}

function pdo_write_csv($db, $query, $to_csvfilename, $headers= array())
{
  try{
    $stmt = $db->prepare($query);
    $stmt->execute();
    $row = $stmt->fetch(PDO::FETCH_OBJ);
    if(empty($row)) {
      throw new Exception("Empty result set");
    }
  } catch(Exception $ex) {
    throw new Exception($ex->getMessage());
  }
  if(empty($headers)) {
    $fields = get_object_vars($row);
    $headers = array_keys($fields);
    $values = array_values($fields);
  }
  $fp = fopen($to_csvfilename, "w");
  fputcsv($fp, $headers);
  fputcsv($fp, $values);
  while(!empty($row) || !feof($fp)) {
    $fields = get_object_vars($row);
    $values = array_values($fields);
    fputcsv($fp, $values);
    $row = $stmt->fetch(PDO::FETCH_OBJ);
  }
}