<body>
  <div class="container">
    <div class="row">
      <h1>Backoffice</h1>
    </div>
    <div class="row">
      <form action="index.php" method="POST">
          <div class="form-group">
              <select name="sites">
                  <option value="">Select a site</option>
                  <?php $prefix_params = [];
                  $wp_config_files = [];
                  foreach(get_wp_folders() as $folder) {
                      $paths = explode(DIRECTORY_SEPARATOR, $folder);
                      $path = array_pop($paths);
                      ?>
                      <?php 
                      $root = search_root_directory($folder);
                      print($root);
                      if(!empty($root)) {
                          $wp_config = $root . '/wp-config.php';
                          $prefix = get_wp_database_prefix($wp_config);
                          array_push($prefix_params, $prefix);
                          array_push($wp_config_files, $wp_config); ?>
                  <option value="<?php echo $path; ?>"><?php echo $path . 
                          " with database prefix: " . $prefix[count($prefix)-2]; ?></option>
                      <?php }
                  }
                  ?>
              </select>
          </div>
          <div class="form-group">
              <input type="text" name="prefix" class="form-control" placeholder="Prefix" />
              <input type="text" name="site" class="form-control" placeholder="Site" />
          </div>
          <div class="form-group">
              <label>
                  Match using SQL query
                  <input type="radio" name="match" value="query" onclick="onClickQuery();" />
              </label>
              <label>
                  Match Only titles
                  <input type="radio" name="match" value="title" onclick="onClickTextArea();" />
              </label>
              <label>
                  Match IDS and Titles
                  <input type="radio" name="match" value="id_title" onclick="onClickTextArea();" />
              </label>
              <label>
                  Match Attributes
                  <input type="radio" name="match" value="attributes" onclick="onClickAttributes();" />
                  <select name="attributes" class="form-control" multiple="true">
                      <option value="postcode">Postcode</option>
                      <option value="website">Website</option>
                      <option value="title">Post Title</option>
                  </select>
              </label>
          </div>
          <div class="form-group">
              <textarea name="query" class="form-control-lg" id="textarea_query" style="display:none">
                  
              </textarea>
              <textarea name="csv" class="form-control-lg" id="textarea_csv" style="display:none">
                  
              </textarea>
          </div>
      </form>
    </div>
  </div>