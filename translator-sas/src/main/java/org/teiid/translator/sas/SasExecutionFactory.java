/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */
package org.teiid.translator.sas;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.language.Argument;
import org.teiid.language.Call;
import org.teiid.language.Command;
import org.teiid.language.Insert;
import org.teiid.metadata.AggregateAttributes;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.*;
import org.teiid.translator.jdbc.*;

@Translator(name="sas-spds", description="A translator for SAS SPDS server")
public class SasExecutionFactory extends JDBCExecutionFactory {

    public static String SAS = "sas"; //$NON-NLS-1$    
    @Override
    public void start() throws TranslatorException {
        super.start();

        ConvertModifier convert = new ConvertModifier();
        convert.addTypeMapping("num", FunctionModifier.INTEGER); //$NON-NLS-1$
        convert.addTypeMapping("char", FunctionModifier.STRING); //$NON-NLS-1$

        registerFunctionModifier(SourceSystemFunctions.CONVERT, convert);

        registerFunctionModifier(SourceSystemFunctions.ACOS, new AliasModifier("arcos")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.ASIN, new AliasModifier("arsin")); //$NON-NLS-1$

        registerFunctionModifier(SourceSystemFunctions.BITAND, new AliasModifier("&")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.BITNOT, new AliasModifier("~")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.BITOR, new AliasModifier("|")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.BITXOR, new AliasModifier("^")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.CURDATE, new AliasModifier("unix_timestamp")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.IFNULL, new AliasModifier("coalesce")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.MOD, new ModFunctionModifier("%", getLanguageFactory(), Arrays.asList(TypeFacility.RUNTIME_TYPES.BIG_INTEGER, TypeFacility.RUNTIME_TYPES.BIG_DECIMAL))); //$NON-NLS-1$


        //addPushDownFunction(SAS, "lower", STRING, STRING); //$NON-NLS-1$

    }    
    
    @Override
    public List<String> getSupportedFunctions() {
        List<String> supportedFunctions = new ArrayList<String>();
        supportedFunctions.addAll(super.getSupportedFunctions());

        supportedFunctions.add(SourceSystemFunctions.ABS);
        supportedFunctions.add(SourceSystemFunctions.ACOS);
        supportedFunctions.add(SourceSystemFunctions.ASIN);
        supportedFunctions.add(SourceSystemFunctions.ASCII);
        supportedFunctions.add(SourceSystemFunctions.ATAN);
        supportedFunctions.add(SourceSystemFunctions.BITAND);
        supportedFunctions.add(SourceSystemFunctions.BITNOT);
        supportedFunctions.add(SourceSystemFunctions.BITOR);
        supportedFunctions.add(SourceSystemFunctions.BITXOR);
        supportedFunctions.add(SourceSystemFunctions.CEILING);
        supportedFunctions.add(SourceSystemFunctions.COALESCE);
        supportedFunctions.add(SourceSystemFunctions.CONCAT);
        supportedFunctions.add(SourceSystemFunctions.COS);
        supportedFunctions.add(SourceSystemFunctions.CONVERT);
        supportedFunctions.add(SourceSystemFunctions.CURDATE);
        supportedFunctions.add(SourceSystemFunctions.CURTIME);
        supportedFunctions.add(SourceSystemFunctions.DEGREES);
        supportedFunctions.add(SourceSystemFunctions.DAYOFMONTH);
        supportedFunctions.add(SourceSystemFunctions.EXP);
        supportedFunctions.add(SourceSystemFunctions.FLOOR);
        supportedFunctions.add(SourceSystemFunctions.HOUR);
        supportedFunctions.add(SourceSystemFunctions.IFNULL);
        supportedFunctions.add(SourceSystemFunctions.LCASE);
        supportedFunctions.add(SourceSystemFunctions.LOCATE);
        supportedFunctions.add(SourceSystemFunctions.LPAD);
        supportedFunctions.add(SourceSystemFunctions.LENGTH);
        supportedFunctions.add(SourceSystemFunctions.LTRIM);
        supportedFunctions.add(SourceSystemFunctions.LOG);
        supportedFunctions.add(SourceSystemFunctions.LOG10);
        supportedFunctions.add(SourceSystemFunctions.MINUTE);
        supportedFunctions.add(SourceSystemFunctions.MOD);
        supportedFunctions.add(SourceSystemFunctions.POWER);
        supportedFunctions.add(SourceSystemFunctions.SECOND);
        supportedFunctions.add(SourceSystemFunctions.SQRT);
        supportedFunctions.add(SourceSystemFunctions.RADIANS);
        supportedFunctions.add(SourceSystemFunctions.ROUND);
        supportedFunctions.add(SourceSystemFunctions.RTRIM);
        supportedFunctions.add(SourceSystemFunctions.RPAD);
        supportedFunctions.add(SourceSystemFunctions.MONTH);
        supportedFunctions.add(SourceSystemFunctions.PI);
        supportedFunctions.add(SourceSystemFunctions.SIN);
        supportedFunctions.add(SourceSystemFunctions.SUBSTRING);
        supportedFunctions.add(SourceSystemFunctions.TAN);
        supportedFunctions.add(SourceSystemFunctions.TRIM);
        supportedFunctions.add(SourceSystemFunctions.UCASE);
        supportedFunctions.add(SourceSystemFunctions.YEAR);
        return supportedFunctions;
    }    
    
    @Override
    public JDBCUpdateExecution createUpdateExecution(Command command, ExecutionContext executionContext, RuntimeMetadata metadata, Connection conn)
            throws TranslatorException {
        if (command instanceof Insert) {
            return new JDBCUpdateExecution(command, conn, executionContext, this);
        }
        throw new TranslatorException(SasPlugin.Event.TEIID24000, SasPlugin.Util.gs(SasPlugin.Event.TEIID24000, command));
    }	
    
    @Override
    public ProcedureExecution createProcedureExecution(Call command, ExecutionContext executionContext, RuntimeMetadata metadata, Connection conn)
            throws TranslatorException {
        throw new TranslatorException(SasPlugin.Event.TEIID24000, SasPlugin.Util.gs(SasPlugin.Event.TEIID24000, command));
    }    
    
    @Override
    public ProcedureExecution createDirectExecution(List<Argument> arguments, Command command, ExecutionContext executionContext, RuntimeMetadata metadata, Connection conn)
            throws TranslatorException {
        throw new TranslatorException(SasPlugin.Event.TEIID24000, SasPlugin.Util.gs(SasPlugin.Event.TEIID24000, command));
    }    

	//@Override
    //public SQLConversionVisitor getSQLConversionVisitor() {
    //}

	@Override
    public boolean useAnsiJoin() {
    	return true;
    }

    @Override
    public boolean supportsBulkUpdate() {
    	return false;
    }

    @Override
    public boolean supportsBatchedUpdates() {
    	return false;
    }

	@Override
    public boolean hasTimeType() {
    	return false;
    }

    @Override
    public String translateLiteralBoolean(Boolean booleanValue) {
        if(booleanValue.booleanValue()) {
            return "true"; //$NON-NLS-1$
        }
        return "false"; //$NON-NLS-1$
    }

    @Override
    public String translateLiteralDate(java.sql.Date dateValue) {
        return '\'' + formatDateValue(dateValue) + '\'';
    }

    @Override
    public String translateLiteralTime(Time timeValue) {
    	if (!hasTimeType()) {
    		return translateLiteralTimestamp(new Timestamp(timeValue.getTime()));
    	}
    	//(DT_WSTR,10)((DT_STR,4,1252)YEAR(Mail_Pce_Dt) + "-" + RIGHT("00" + (DT_STR,2,1252)MONTH(Mail_Pce_Dt),2) + "-" + RIGHT("00" + (DT_STR,2,1252)DAY(Mail_Pce_Dt),2))    	
    	return '\'' + formatDateValue(timeValue) + '\'';
    }

    @Override
    public String translateLiteralTimestamp(Timestamp timestampValue) {
        return '\'' + formatDateValue(timestampValue) + '\'';
    }


    @Override
    protected JDBCMetdataProcessor createMetadataProcessor() {
        return getMetadataProcessor();
    }
    
    public SasMetadataProcessor getMetadataProcessor(){
        return new SasMetadataProcessor();
    }

    @Override
    public Object retrieveValue(ResultSet results, int columnIndex, Class<?> expectedType) throws SQLException {
    	return super.retrieveValue(results, columnIndex, expectedType);
    }
    
    protected FunctionMethod addAggregatePushDownFunction(String qualifier, String name, String returnType, String...paramTypes) {
    	FunctionMethod method = addPushDownFunction(qualifier, name, returnType, paramTypes);
    	AggregateAttributes attr = new AggregateAttributes();
    	attr.setAnalytic(true);
    	method.setAggregateAttributes(attr);
    	return method;
    }
}
