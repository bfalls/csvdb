<?php
# csvdb - A module for managing CSV files with limited SQL features

# The CSV heaader is important, the syntax is:
# id, fieldname[;UNIQUE|NOT NULL], ...
# Example header:
#      42,lockey;UNIQUE,location,city;NOT NULL,zip
#
# The first data field is the record ID. It is always considered the
# primary key.
# The first header field is the next ID, when a new record is added this is
# used as the record id and is incremented to the next id. 
#
# Note: If you pass in a record with a missing key then its value in the CSV
# file will be empty.

function icsvdbProcessMetadata($hdrs) {
    $hasConstraints = false;
    $idx = 0;
    $uniqueIdxs = [];
    $hdrnames = [];
    foreach($hdrs as $hdr) { // N.B. $hdrs was already shifted!!!
        $cnhdr = explode(';', $hdr);
        $hdrnames[] = $cnhdr[0]; // build list of header names only
        if (count($cnhdr) > 1) {
            $hasConstraints = true;
            if ($cnhdr[1] == 'UNIQUE') {
                $uniqueIdxs[] = $idx; // add idx to unique list
            }
        }
        $idx++;
    }
    return array('hasConstraints'=>$hasConstraints,'uniqueIndices'=>$uniqueIdxs,'headerNames'=>$hdrnames);
}

# csvdbCreateTable - writes the csvdb header in a new file
# param $fn the filename of the database
# param $schema the array of fields
# Example: csvdbCreateTable('table.csv', array(array('name'=>'street'),array('name'=>'zip','constraint'=>'NOT NULL')));
function csvdbCreateTable($fn, $schema) {
    if (gettype($schema) !== 'array' || count($schema) === 0) {
        return array('code'=>500, 'value'=>'Invalid schema.');
    }
    if (is_file($fn)) {
        return array('code'=>409, 'value'=>'Table \'' . $fn . '\' already exists.');
    }
    $hdr = array('      1');
    foreach($schema as $col) {
        if (! array_key_exists('name', $col)) {
            return array('code'=>500, 'value'=>'Invalid schema.');
        }
        $colname = $name = $col['name'];
        if (array_key_exists('constraint', $col)) {
            $constraint = $col['constraint']; # NOT NULL, DEFAULT, UNIQUE
            if (!($constraint === 'NOT NULL' || 
                $constraint === 'UNIQUE' ||
                substr($constraint, 0, 8) === 'DEFAULT ')) {
                return array('code'=>400, 'value'=>'Invalid constraint. (' . $constraint . ')');
            }
            $colname .= ';' . $constraint;
        }
        $hdr[] = $colname;
    }
    $f = fopen($fn, 'w');
    flock($f, LOCK_EX);
    fputcsv($f, $hdr);
    flock($f, LOCK_UN);
	fclose($f);
    return array('code'=>201, 'value'=>'');
}

# csvdbSelect - selects records returning requested columns
# with requested filters applied and in requested order.
# Example:
#   csvdbSelect('/file.csv', array('fname','lname'), array(array('fname','=','Jane')))) 
function csvdbSelect($fn, $cols = null, $wheres = null)
{
	$f = fopen($fn, 'r');
	flock($f, LOCK_EX); # append , 1) at the end????
	$hdrs = fgetcsv($f);
    $hdridxs = [];
    $idx = 0;
    foreach($hdrs as $hdr) {
        $cnhdr = explode(';', $hdr);
        if (in_array($cnhdr[0], $cols)) {
            $hdridxs[] = $idx;
        }
        $idx++;
    }
    var_dump($hdridxs);
    
    $results = [];
    // loop through the whole database; EXPENSIVE!!!
    while (($row = fgetcsv($f)) !== false) {
        var_dump($row);
        foreach($hdridxs as $hdridx) {
            $results[] = $row[$hdridx];
        }
    }
    
	flock($f, LOCK_UN);
	fclose($f);
    return array('code'=>200, 'value'=>$results);
}

# csvdbAddRecord - adds a record to the end of the CSV file and
# updates the next record count.
# Parameters:
#   $fn - name of the CSV file
#   $r - record to be added
# Only fields from the header in the file will be added from the given
# record.
# Returns:
#   An HTTP response code. 201 Created on sucess, 409 Conflict on constraint failure
# Example:
#   csvdbAddRecord('/data/2013/file.csv', array('fname'=>'Bill','lname'=>'Smith'));
function csvdbAddRecord($fn, $r)
{
	# $r = array('key' => '4c', 'name' => 'test');
	# $fn = 'divs.csv';
	$f = fopen($fn, 'r+');
	flock($f, LOCK_EX); # append , 1) at the end????
	$hdrs = fgetcsv($f);
	$rn = intval(array_shift($hdrs)); # pull next record id off and 
	$newrec = array();
	$newrec[] = $rn; # the new record id

    // check headers for constraints
    $mds = icsvdbProcessMetadata($hdrs);
    $hdrnames = $mds['headerNames'];
    if ($mds['hasConstraints']) {
        $uniqueIdxs = $mds['uniqueIndices'];
        
        // loop through the whole database; EXPENSIVE!!!
        while (($row = fgetcsv($f)) !== false) {
            foreach($uniqueIdxs as $uidx) { // check all UNIQUE constraints
                if ($row[1+$uidx] == $r[$hdrnames[$uidx]]) {
                    return array('code'=>409, 'value'=>'IntegrityError: UNIQUE constraint failed: ' . $hdrnames[$uidx]); // failed unique constraint, 409 CONFLICT
                }
            }
        }
    }
    
    # create new record
	foreach($hdrnames as $hdr) { # loop in order of headers
		$newrec[] = $r[$hdr];
	}

	// write the next record id in header
	fseek($f, 0, SEEK_SET);
	fwrite($f, sprintf('%7u', $rn + 1));
	fseek($f, 0, SEEK_END);
	fputcsv($f, $newrec);
	flock($f, LOCK_UN);
	fclose($f);
    return array('code'=>201, 'value'=>$rn); // Created, and new ID
}

# csvdbUpdateRecord - updates a record in the CSV file.
# Parameters:
#   $fn - name of the CSV file
#   $r - record to be added
# Only fields from the header in the file will be updated from the given
# record
# Example:
#   csvdbUpdateRecord('/data/2013/file.csv', array('id'=>5,'fname'=>'Bill','lname'=>'Jones'));
function csvdbUpdateRecord($fn, $r)
{
	$f = fopen($fn, 'r+');
	flock($f, LOCK_EX); # append , 1) at the end????
    $rid = $r['id'];
	$sid = $rid . ',';
	$found = false;
	$hdrs = fgetcsv($f);
	array_shift($hdrs); # get rid of next id hdr.
	$slen = ftell($f);
    $foundpos = $slen;
    $endpos = 0;
    $ierr = '';

    // check headers for constraints
    $mds = icsvdbProcessMetadata($hdrs);
    $hdrnames = $mds['headerNames'];
    if ($mds['hasConstraints']) {
        $uniqueIdxs = $mds['uniqueIndices'];
        
        // loop through the whole database; EXPENSIVE!!!
        while (($row = fgetcsv($f)) !== false) {
            if ($row[0] == $rid) { // don't test the record being replaced
                $found = true;
                $endpos = ftell($f); // EXPENSIVE!!!
            } else { 
                foreach($uniqueIdxs as $uidx) { // check all UNIQUE constraints
                    if ($row[1+$uidx] == $r[$hdrnames[$uidx]]) {
                        $ierr = 'IntegrityError: UNIQUE constraint failed: ' . $hdrnames[$uidx];
                        break;
                    }
                }
                if (! $found) {
                    $foundpos = ftell($f); // EXPENSIVE!!!
                }
            }
        }
    }

	# move remaining
	if ($found)
	{
        if (strlen($ierr) > 0) {
            flock($f, LOCK_UN);
            fclose($f);
            return array('code'=>409, 'value'=>$ierr); // failed unique constraint, 409 CONFLICT
        }
        fseek($f, $endpos, SEEK_SET);
		$s = fread($f, filesize($fn));
		fseek($f, $foundpos, SEEK_SET);
		fwrite($f, $s);

		# add updated record
		$editrec = array();
		$editrec[] = $rid; # same record id
		foreach($hdrnames as $hdr) # loop in order of headers
		{
			$editrec[] = $r[$hdr];
		}

		fputcsv($f, $editrec);

		# truncate if updated record is shorter
		ftruncate($f, ftell($f));
	} else {
        flock($f, LOCK_UN);
        fclose($f);
        return array('code'=>404, 'value'=>''); // Not Found
    }

	flock($f, LOCK_UN);
	fclose($f);
    return array('code'=>200, 'value'=>''); // Not Found
}

# csvdbDeleteRecord - deletes a record in the CSV file.
# Parameters:
#   $fn - name of the CSV file
#   $sid - id of the record to be deleted
# Example:
#   csvdbDeleteRecord('/data/2013/file.csv', '55');
function csvdbDeleteRecord($fn, $rid)
{
	$f = fopen($fn, 'r+');
	flock($f, LOCK_EX); # append , 1) at the end????
	$sid = $rid . ',';
	$found = false;
	$hdrs = fgets($f);
	$slen = strlen($hdrs);

	# find the record
	while (($s = fgets($f)) !== false) {
		if (0 === strpos($s, $sid)) {
			$found = true;
			break;
		}
		$slen += strlen($s);
	}

	# move remaining
	if ($found) {
		$s = fread($f, filesize($fn));
		fseek($f, $slen, SEEK_SET);
        $row = fgetcsv($f); // read row
        fseek($f, $slen, SEEK_SET);
		fwrite($f, $s); // write the rest
		ftruncate($f, ftell($f));
	}

	flock($f, LOCK_UN);
	fclose($f);
    if ($found) {
        return array('code'=>200, 'value'=>$row);
    }
    return array('code'=>404, 'value'=>'');
}

?>
