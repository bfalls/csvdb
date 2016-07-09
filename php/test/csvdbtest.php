<?php
// Run this test case with:
// phpunit csvdbtest.php

require '../csvdb.php';

class CsvDbTest extends PHPUnit_Framework_TestCase
{
    protected $filename;
        
    protected function tearDown()
    {
        // if (file_exists($fn)) {
        //     unlink($fn);
        // }
    }
    
    protected function setUp()
    {
        $this->filename = dirname(__FILE__) . '/test.csv';
        $fn = $this->filename;
        if (file_exists($fn)) {
            unlink($fn);
        }
    }

    public function testCreateTable()
    {
        $fn = $this->filename;
        $result = csvdbCreateTable($fn, array(array('name'=>'street'),array('name'=>'zip','constraint'=>'NOT NULL')));
        $this->assertSame($result['code'], 201, $result['value']);
        $this->assertFileExists($fn);

        // table already exists
        $result = csvdbCreateTable($fn, array(array('name'=>'street'),array('name'=>'zip','constraint'=>'NOT NULL')));
        $this->assertSame($result['code'], 409, $result['value']);
        $this->assertTrue(unlink($fn));

        // bad constraint
        $result = csvdbCreateTable($fn, array(array('name'=>'street'),array('name'=>'zip','constraint'=>'BLAH')));
        $this->assertSame($result['code'], 400, $result['value']);
        $this->assertFileNotExists($fn);
        
        // no name in schema field
        $result = csvdbCreateTable($fn, array(array('name'=>'street'),array('noname'=>'zip','constraint'=>'BLAH')));
        $this->assertSame($result['code'], 500, $result['value']);
        $this->assertFileNotExists($fn);

        // no schema
        $result = csvdbCreateTable($fn, null);
        $this->assertSame($result['code'], 500, $result['value']);
        $this->assertFileNotExists($fn);
        $result = csvdbCreateTable($fn, array());
        $this->assertSame($result['code'], 500, $result['value']);
        $this->assertFileNotExists($fn);
        
    }

    public function testSelect()
    {
        $fn = $this->filename;
        $result = csvdbCreateTable($fn, [['name'=>'fname'],['name'=>'lname'],['name'=>'zip']]);
        $this->assertSame($result['code'], 201, $result['value']);
        $this->assertFileExists($fn);
        $result = csvdbInsert($fn, ['fname'=>'Barnaby','lname'=>'Falls','zip'=>'91234']);
        $this->assertSame($result['code'], 201, $result['value']);

        // select first record
        $result = csvdbSelect($fn, $result['value']);
        $this->assertSame($result['code'], 200, $result['value']);
        $rec = $result['value'];
        $this->assertSame($rec['fname'], 'Barnaby', $rec['fname']);
        $this->assertSame($rec['lname'], 'Falls', $rec['lname']);
        $this->assertSame($rec['zip'], '91234', $rec['zip']);

        // select second record
        $result = csvdbInsert($fn, ['fname'=>'Yulia','lname'=>'Falls','zip'=>'94321']);
        $this->assertSame($result['code'], 201, $result['value']);
        $result = csvdbSelect($fn, $result['value']);
        $this->assertSame($result['code'], 200, $result['value']);
        $rec = $result['value'];
        $this->assertSame($rec['fname'], 'Yulia', $rec['fname']);
        $this->assertSame($rec['lname'], 'Falls', $rec['lname']);
        $this->assertSame($rec['zip'], '94321', $rec['zip']);

        // test select failure
        $result = csvdbSelect($fn, 3202);
        $this->assertSame($result['code'], 404, $result['value']);

        // select whole collection
        $result = csvdbSelect($fn);
        $this->assertSame($result['code'], 200, $result['value']);
        $coll = $result['value'];
        $this->assertSame(count($coll), 2, $result['value']);
        $rec = $coll[0];
        $this->assertSame($rec['fname'], 'Barnaby', $rec['fname']);
        $this->assertSame($rec['lname'], 'Falls', $rec['lname']);
        $this->assertSame($rec['zip'], '91234', $rec['zip']);
        $rec = $coll[1];
        $this->assertSame($rec['fname'], 'Yulia', $rec['fname']);
        $this->assertSame($rec['lname'], 'Falls', $rec['lname']);
        $this->assertSame($rec['zip'], '94321', $rec['zip']);



        
    }

    public function testDelete()
    {
        $fn = $this->filename;
        $result = csvdbCreateTable($fn, [['name'=>'fname'],['name'=>'lname'],['name'=>'zip']]);
        $result = csvdbInsert($fn, ['fname'=>'Barnaby','lname'=>'Falls','zip'=>'91234']);
        $result = csvdbInsert($fn, ['fname'=>'Yulia','lname'=>'Falls','zip'=>'94321']);
        $result = csvdbSelect($fn);
        $coll = $result['value'];
        $this->assertSame(count($coll), 2, $result['value']);
        $result = csvdbDeleteRecord($fn, 2);
        $result = csvdbSelect($fn);
        $coll = $result['value'];
        $this->assertSame(count($coll), 1, $result['value']);
        $result = csvdbDeleteRecord($fn, 1);
        $result = csvdbSelect($fn);
        $coll = $result['value'];
        $this->assertSame(count($coll), 0, $result['value']);
        $bresult = csvdbInsert($fn, ['fname'=>'Barnaby','lname'=>'Falls','zip'=>'91234']);
        $yresult = csvdbInsert($fn, ['fname'=>'Yulia','lname'=>'Falls','zip'=>'94321']);
        $nresult = csvdbInsert($fn, ['fname'=>'Nick','lname'=>'Falls','zip'=>'94501']);
        $result = csvdbSelect($fn);
        $coll = $result['value'];
        $this->assertSame(count($coll), 3, $result['value']);
        $result = csvdbDeleteRecord($fn, $yresult['value']);
        $result = csvdbSelect($fn);
        $coll = $result['value'];
        $this->assertSame(count($coll), 2, $result['value']);
        $result = csvdbDeleteRecord($fn, $bresult['value']);
        $result = csvdbSelect($fn);
        $coll = $result['value'];
        $rec = $coll[0];
        $this->assertSame(count($coll), 1, $result['value']);
        $this->assertSame($rec['fname'], 'Nick', $result['value']);

        $result = csvdbDeleteRecord($fn, 3202);
        $this->assertSame($result['code'], 404, $result['value']);
    }

    public function testUpdate()
    {
        $fn = $this->filename;
        $result = csvdbCreateTable($fn, [['name'=>'fname'],['name'=>'lname'],['name'=>'zip']]);
        $bresult = csvdbInsert($fn, ['fname'=>'Barnaby','lname'=>'Falls','zip'=>'91234']);
        $yresult = csvdbInsert($fn, ['fname'=>'Yulia','lname'=>'Falls','zip'=>'94321']);
        // update first record
        $result = csvdbUpdate($fn, ['id'=>$bresult['value'],'fname'=>'Nick','lname'=>'Falls','zip'=>'94501']);
        $result = csvdbSelect($fn, $bresult['value']);
        $rec = $result['value'];
        $this->assertSame($rec['fname'], 'Nick', $rec);
        $this->assertSame($rec['zip'], '94501', $rec);
        // update last record
        $result = csvdbUpdate($fn, ['id'=>$bresult['value'],'fname'=>'Barnaby','lname'=>'Falls','zip'=>'91234']);
        $result = csvdbSelect($fn, $bresult['value']);
        $rec = $result['value'];
        $this->assertSame($rec['fname'], 'Barnaby', $rec);
        $this->assertSame($rec['zip'], '91234', $rec);

        // add one rec and update the middle
        $nresult = csvdbInsert($fn, ['fname'=>'Nick','lname'=>'Falls','zip'=>'94501']);
        $result = csvdbUpdate($fn, ['id'=>$bresult['value'],'fname'=>'Joe','lname'=>'Montana','zip'=>'95003']);
        $result = csvdbSelect($fn, $bresult['value']);
        $rec = $result['value'];
        $this->assertSame($rec['fname'], 'Joe', $rec);
        $this->assertSame($rec['zip'], '95003', $rec);

        $result = csvdbSelect($fn);
        $coll = $result['value'];
        $this->assertSame(count($coll), 3, $coll);

    }
}
?>
