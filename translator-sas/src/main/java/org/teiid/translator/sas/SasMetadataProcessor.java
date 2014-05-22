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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.Column;
import org.teiid.metadata.Column.SearchType;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.JDBCMetdataProcessor;

@SuppressWarnings("nls")
public class SasMetadataProcessor extends JDBCMetdataProcessor {
    private Pattern tableNamePattern;
    private Pattern excludeTables;
    private String schemaName = "X0000001";
    
	public void process(MetadataFactory metadataFactory, Connection conn)	throws TranslatorException {
		try {
		    getConnectorMetadata(conn, metadataFactory);
        } catch (SQLException e) {
            throw new TranslatorException(e);
        }
	}
    
    @Override
    public void getConnectorMetadata(Connection conn, MetadataFactory metadataFactory)
            throws SQLException {
        List<TableInfo> tablesInfo = getTables(conn);
        Map<String, Table> tables = new HashMap<String, Table>();
        
        for (TableInfo tableInfo:tablesInfo) {
            if (shouldExclude(tableInfo)) {
                LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Excluding table based on pattern = ", tableInfo.getFullName());
                continue;
            }
            
            if (shouldInclude(tableInfo)) {
                Table table = addTable(metadataFactory, null, tableInfo.getOwner(), tableInfo.getName(), null, tableInfo.getFullName());
                if (table == null) {
                    continue;
                }
                table.setSupportsUpdate(false);
                tables.put(tableInfo.getFullName(), table);             
            }
        }
        
        // add columns
        addColumns(tables, conn, metadataFactory);  
        
        // remove empty views
        for (Table t:metadataFactory.getSchema().getTables().values()) {
            if (t.getColumns() == null || t.getColumns().isEmpty()) {
                LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Table or view is not valid, no colums found = ", t.getName());
                metadataFactory.addColumn("dummy", TypeFacility.RUNTIME_NAMES.STRING, t);
            }
        }
    }
    
    protected boolean shouldExclude(TableInfo table) {
        return excludeTables != null && excludeTables.matcher(table.getName()).matches();
    }
    
    protected boolean shouldInclude(TableInfo table) {
        if (this.tableNamePattern != null) {
            return this.tableNamePattern.matcher(table.getName()).matches();
        }
        return true;
    }    
    
    @Override
    public void setExcludeTables(String excludeTables) {
        this.excludeTables = Pattern.compile(excludeTables, Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
        super.setExcludeTables(excludeTables);
    }
    
    @Override
    public void setTableNamePattern(String tableNamePattern) {
        this.tableNamePattern = Pattern.compile(tableNamePattern, Pattern.DOTALL | Pattern.CASE_INSENSITIVE);;
        super.setTableNamePattern(tableNamePattern);
    }
    
    public void setSchemaName(String name) {
        this.schemaName = name;
        super.setSchemaPattern(name);
    }    
    
    static class TableInfo {
        private String owner;
        private String name;

        public TableInfo(String owner, String name) {
            this.owner = owner;
            this.name = name;
        }
        
        public String getOwner() {
            return owner;
        }   
        public String getName() {
            return name;
        }        
        public String getFullName() {
            return this.owner+"."+name;
        }
        
        @Override
        public String toString() {
            return getFullName();
        }
    }
    
	private List<TableInfo> getTables(Connection conn) throws SQLException {
	    String sql = "SELECT \n" + 
	    		"LIBNAME AS owner,\n" + 
	    		"MEMNAME AS name,\n" + 
	    		"MEMTYPE AS type,\n" + 
	    		"MEMNAME AS remarks " +
	    		"FROM dictionary.tables AS tbl\n" + 
	    		"WHERE ( memtype = 'DATA' OR memtype = 'VIEW')\n" + 
	    		"AND (tbl.LIBNAME EQ '"+this.schemaName+"')\n" + 
	    		"ORDER BY type, owner, name";
	    
	    LogManager.logDetail(LogConstants.CTX_CONNECTOR, sql);
	    
		ArrayList<TableInfo> tables = new ArrayList<TableInfo>();
		Statement stmt = conn.createStatement();
		ResultSet rs =  stmt.executeQuery(sql); //$NON-NLS-1$
		while (rs.next()){
			tables.add(new TableInfo(rs.getString(1).trim(), rs.getString(2).trim()));
		}
		rs.close();
		
		
		return tables;
	}

	private String getRuntimeType(String name, String type, String length) {
	    
	    if (name.toUpperCase().endsWith("_DT")) {
	        return TypeFacility.RUNTIME_NAMES.DATE;
	    }
	    else if (name.toUpperCase().endsWith("DT_TM")) {
	        return TypeFacility.RUNTIME_NAMES.TIMESTAMP;
	    }
	    else if (name.toUpperCase().endsWith("_TM")) {
	        return TypeFacility.RUNTIME_NAMES.TIME;
	    }
	    else if (type.equalsIgnoreCase("num") && length.equals("8.0")) { //$NON-NLS-1$
			return TypeFacility.RUNTIME_NAMES.INTEGER;
		}
		else if (type.equalsIgnoreCase("char")) { //$NON-NLS-1$
			return TypeFacility.RUNTIME_NAMES.STRING;
		}
		return TypeFacility.RUNTIME_NAMES.STRING;
	}

	private void addColumns(Map<String, Table> tableMap, Connection conn, MetadataFactory metadataFactory) throws SQLException {
	    /*
         (1)libname char(8) label='Library Name', 
         (2)memname char(32) label='Member Name', 
         (3)memtype char(8) label='Member Type', 
         (4)name char(32) label='Column Name', 
         (5)type char(4) label='Column Type', 
         (6)length num label='Column Length', 
         (7)npos num label='Column Position', 
         (8)varnum num label='Column Number in Table', 
         (9)label char(256) label='Column Label'
   		 */
	    StringBuilder sb = new StringBuilder();
	    sb.append("SELECT * FROM dictionary.columns WHERE memname IN (");
	    List<Table> tables = new ArrayList<Table>(tableMap.values());
	    for (int i = 0; i < tables.size(); i++) {
	        Table t = tables.get(i);
	        sb.append("'").append(t.getName()).append("'");
	        if (i < tables.size()-1) {
	            sb.append(",");
	        }
	    }
	    sb.append(")");
	    
	    String sql = sb.toString();
	    LogManager.logDetail(LogConstants.CTX_CONNECTOR, sql);
	    
		Statement stmt = conn.createStatement();
		ResultSet rs =  stmt.executeQuery(sql); //$NON-NLS-1$
		while (rs.next()){
		    
		    TableInfo colTable = new TableInfo(rs.getString(1).trim(), rs.getString(2).trim());
		    
			String name = rs.getString(4);
			String type = rs.getString(5);
			String length = rs.getString(6);
			String label = rs.getString(9);
			
			if (name != null) {
			    name = name.trim();
			}
			if (type != null) {
				type = type.trim(); 
			}
			if (length != null) {
			    length = length.trim();
			}
			if (label != null) {
			    label = label.trim();
			}
			
            if (tableMap.get(colTable.getFullName()) == null) {
                continue;
            }
			
			String runtimeType = getRuntimeType(name, type, length);

			Table table = tableMap.get(colTable.getFullName());
			if (table.getColumnByName(name) != null) {
			    LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Column ", name, " already found in table ", table.getName());
			    continue;
			}
			
			Column column = metadataFactory.addColumn(name, runtimeType, table);
			column.setNameInSource(name);
			column.setLength(Double.valueOf(length).intValue());
			column.setAnnotation(label);
			column.setUpdatable(false);
			column.setSearchType(SearchType.Searchable);
		}
		rs.close();
	}
}
