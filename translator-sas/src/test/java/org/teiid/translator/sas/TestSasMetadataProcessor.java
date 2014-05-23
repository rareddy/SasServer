package org.teiid.translator.sas;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.sas.SasMetadataProcessor.TableInfo;

@SuppressWarnings("nls")
public class TestSasMetadataProcessor {

    @Test
    public void testShouldInclude() {
        SasMetadataProcessor processor = new SasMetadataProcessor();
        processor.setIncludeTablePattern("PL0613.*");
        assertTrue(processor.shouldInclude(new TableInfo("x00001", "PL0613_TJATITA_INIT")));
        
        processor.setIncludeTablePattern("PL0613_TJATITA_INIT");
        assertTrue(processor.shouldInclude(new TableInfo("x00001", "PL0613_TJATITA_INIT")));
    }
    
    @Test
    public void testShouldInclude2() {
        SasMetadataProcessor processor = new SasMetadataProcessor();
        processor.setIncludeTablePattern("PL0613_TJATITA_INIT|PL0613_TJATUOP");
        assertTrue(processor.shouldInclude(new TableInfo("x00001", "PL0613_TJATITA_INIT")));
        assertFalse(processor.shouldInclude(new TableInfo("x00001", "PL0613_TJATITA")));
    }    
    
    @Test
    public void testShouldExclude() {
        SasMetadataProcessor processor = new SasMetadataProcessor();
        processor.setExcludeTablePattern("PL0613.*");
        assertTrue(processor.shouldExclude(new TableInfo("x00001", "PL0613_TJATITA_INIT")));
        
        processor.setExcludeTablePattern("PL0613_TJATITA_INIT");
        assertFalse(processor.shouldExclude(new TableInfo("x00001", "PL0613_TJATITA")));
    }   
    
    @Test
    public void testCheckTime() {
        SasMetadataProcessor processor = new SasMetadataProcessor();
        assertEquals(TypeFacility.RUNTIME_NAMES.DATE, processor.getRuntimeType("FOO_dt", "num", "8.0"));
        assertEquals(TypeFacility.RUNTIME_NAMES.TIME, processor.getRuntimeType("FOO_Tm", "num", "8.0"));
        assertEquals(TypeFacility.RUNTIME_NAMES.TIMESTAMP, processor.getRuntimeType("FOO_dt_TM", "num", "8.0"));
    }     
}
