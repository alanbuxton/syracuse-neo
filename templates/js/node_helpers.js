function convert_uri(value) {
  if (value.startsWith("https://1145.am")) {
    var regex = /^https:\/\/1145.am/i ;
    tmp_val = value.replace(regex, location.protocol + "//" + location.host + "/resource/1145.am");
    return tmp_val;
  } else {
    return value;
  }
}

function showItemDetails(item_id, lookup_dict, node_or_edge) {
	item_vals = Object.entries(lookup_dict[item_id]);
  entity_type = '';
  text = "";
  documentURL = "";
  archive_org_page_url = "";
  archive_org_list_url = "";

  for (const [key, value] of item_vals) {
    if ( key.toLowerCase().endsWith("url") | (key.toLowerCase().endsWith("uri")) ) {
      if (Array.isArray(value)) {
        val_as_arr = value
      } else {
        val_as_arr = [value]
      }
      for (val_id in val_as_arr) {
          val = val_as_arr[val_id]
          uri_target = convert_uri(val)
          text = text + "<strong>" + titleCase(key) + "</strong>: <a href='" + uri_target + "' target='_blank'>" + val + "</a></br>";
      }
    } else {
      text = text + "<strong>" + titleCase(key) + "</strong>: " + value + "</br>";
    }
    if (key === 'entity_type') {
      entity_type = value;
    }
    // if (key === 'document_url') {
    //   documentURL = value;
    // }
    if (key === 'internet_archive_page_url') {
      archive_org_page_url = value;
    }
    // if (key === 'internet_archive_list_url') {
    //   archive_org_list_url = value;
    // }
  }

  if (archive_org_page_url !== '') {
    text = text + "<em>Archive.org page links will work if the page has already been archived by archive.org, but we can't guarantee which pages archive.org has archived so far.</em>"
  }

  text = text + "<br/>";

  if (entity_type == 'Organization') {
    path = item_id.replace("https://","");
    path = path.replace("http://","");
    text = text + "<p><a href='/organization/linkages/uri/" + path + "'>Show a new visualization centered on this organization</a></p>";
  } else if (entity_type == 'Cluster') {
    text = text + "<p>This is the central organization for this linkages graph</p>";
  }

  text = text + error_form(node_or_edge, item_id);
	return text;
}

function error_form(node_or_edge, unique_id) {
  text = "<form method='post' action='/feedbacks/create' class='form-inline'>";
  text = text + "See something unexpected or wrong about this item? If so please use the form below to notify us of the issue:</p>";
  text = text + "<input type='hidden' name='csrfmiddlewaretoken' value='" + Cookies.get('csrftoken') + "'>"
  text = text + "<input type='hidden' name='node_or_edge' value='" + node_or_edge+ "'/><input type='hidden' value='";
  text = text + unique_id;
  text = text + "' name='idval'/><div class='mb-3'><label for='reasonTextArea' class='form-label'><strong>Reason:</strong></label>";
  text = text + "<textarea name='reason' id='reasonTextArea' class='form-control' rows='3'></textarea></div><input type='submit' value='Submit Suggestion'/></form>";
  return text

}

// From https://stackoverflow.com/a/46501455/7414500
function capitalize(str) {
  return str.charAt(0).toUpperCase() + str.substring(1, str.length).toLowerCase();
}

function titleCase(str) {
  s1 = str.replace(/[^\ \/\-\_]+/g, capitalize).replace(/_/g," ");
  return s1.replace("Uri","URI").replace("Url","URL").replace("Rdf","RDF");
}
