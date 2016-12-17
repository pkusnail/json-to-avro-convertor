package com.AlejandroSeaah;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.allegro.schema.json2avro.converter.JsonAvroConverter;

import static org.junit.Assert.assertEquals;

/**
 * Created by alejandroseaah on 16/12/17.
 */
public class AvroSchemaAdaptorTest {
    private  static Logger logger = LoggerFactory.getLogger(AvroSchemaAdaptorTest.class);
    @Test
    public void Test() throws DataProcessException{

        String json = "{\"page\":\"FirstJson\",\"indexPath\":\"[-1,1]\",\"eventTime\":\"1480168647142\",\"view\":{\"path\":\"DecorView\\/LinearLayout[0]\\/FrameLayout[0]\\/RelativeLayout[0]\\/TabHost[0]\\/RelativeLayout[0]\\/FrameLayout[0]\\/DecorView[1]\\/LinearLayout[0]\\/FrameLayout[0]\\/LinearLayout[0]\\/CPRefreshableView[0]\\/ListView[0]\\/LinearLayout[-1,1]\",\"title\":\"\",\"frame\":\"{0,220,480,128}\",\"viewId\":\"df2f39d\",\"viewClass\":\"LinearLayout\"}}";
        String json2= "{\"page\":\"SecondJson\",\"indexPath\":\"[-1,2]\",\"eventTime\":\"1480168635950\",\"view\":{\"path\":\"DecorView\\/LinearLayout[0]\\/FrameLayout[0]\\/RelativeLayout[0]\\/LinearLayout[0]\\/FrameLayout[0]\\/NoSaveStateFrameLayout[0]\\/LinearLayout[0]\\/CPRefreshableView[0]\\/ClassifiedListView[0]\\/ListViewEx[0]\\/LinearLayout[-1,2]\",\"title\":\"\",\"frame\":\"{0,808,1080,236}\",\"viewId\":\"7194afeb\",\"viewClass\":\"LinearLayout\"}}";
        String autoAvroSchema = null;
        try{
            autoAvroSchema = AvroSchemaAdaptor.avroSchemaAutoAdaption(json);
        }catch (Exception e){
            e.printStackTrace();
        }
        Schema schema = new Schema.Parser().parse(autoAvroSchema);
        logger.info(autoAvroSchema);

        JsonAvroConverter converter = new JsonAvroConverter();
        GenericRecord gr = converter.convertToGenericDataRecord(json2.getBytes(), schema);
        assertEquals(gr.get("page"),"SecondJson");
        assertEquals("[-1,2]",gr.get("indexPath"));
    }
}
