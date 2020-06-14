package org.spider.topupservices.DataSources;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.naming.InitialContext;
import javax.sql.DataSource;

import org.spider.topupservices.Utilities.NullPointerExceptionHandler;

public class WeTopUpDS {
	DataSource dataStore;
	Connection connection; 
	PreparedStatement preparedStatement; 
	InitialContext initialContext;
	ResultSet resultSet;
	CallableStatement callableStatement;

	public WeTopUpDS(){
		super();
		try {
			
			initialContext = new InitialContext();
//			dataStore = (DataSource)initialContext.lookup( "java:/WeTopUpSandboxDS" );	//	sandbox
			dataStore = (DataSource)initialContext.lookup( "java:/WeTopUpDS" );  // live
			connection = dataStore.getConnection();
		}catch(Exception e){
			System.out.println("Exception thrown " +e);
		}
	}
	public CallableStatement prepareCall(String statement) throws SQLException {
		this.callableStatement=this.getConnection().prepareCall(statement);
		return this.callableStatement;
	}
	public CallableStatement getCallableStatement() {
		return this.callableStatement;
	}
	public void setCallableStatement(CallableStatement callableStatement) {
		this.callableStatement = callableStatement;
	}
	public PreparedStatement prepareStatement(String statement) throws SQLException {
		this.preparedStatement=this.getConnection().prepareStatement(statement);
		return this.preparedStatement;
	}
	public PreparedStatement newPrepareStatement(String statement) throws SQLException {
		PreparedStatement preparedStatement=this.getConnection().prepareStatement(statement);
		return preparedStatement;
	}
	public PreparedStatement newPrepareStatement(String statement,boolean returnGeneratedKeys) throws SQLException {
		PreparedStatement preparedStatement=this.getConnection().prepareStatement(statement,PreparedStatement.RETURN_GENERATED_KEYS);
		return preparedStatement;
	}
	public PreparedStatement prepareStatement(String statement,boolean returnGeneratedKeys) throws SQLException {
		if(returnGeneratedKeys) {
			this.preparedStatement=this.getConnection().prepareStatement(statement,PreparedStatement.RETURN_GENERATED_KEYS);
		}else {
			this.preparedStatement=this.getConnection().prepareStatement(statement);
		}
		return this.preparedStatement;
	}
	public ResultSet getGeneratedKeys() throws SQLException {
		return this.preparedStatement.getGeneratedKeys();
	}
	public ResultSet executeQuery() throws SQLException {
		this.resultSet= this.preparedStatement.executeQuery();
		return this.resultSet;
	}
	
	public boolean execute() throws SQLException {
		return this.preparedStatement.execute();
	}
	
	public long executeUpdate() throws SQLException {
		return this.preparedStatement.executeUpdate();
	}
	
	public int executeUpdate_v2() throws SQLException {
		return this.preparedStatement.executeUpdate();
	}
	
	public void closePreparedStatement() throws SQLException {
		this.preparedStatement.close();
	}
	public void closeResultSet() throws SQLException {
		this.resultSet.close();
	}
	public void closeCallableStatement() throws SQLException {
		this.callableStatement.close();
	}
	public boolean isCallableStatementClosed() throws SQLException {
		if(this.callableStatement==null) {
			return true;
		}else {
			return this.callableStatement.isClosed();
		}
	}
	public boolean isPreparedStatementClosed() throws SQLException {
		if(this.preparedStatement==null) {
			return true;
		}else {
			return this.preparedStatement.isClosed();
		}
	}
	public boolean isResultSetClosed() throws SQLException {
		if(this.resultSet==null) {
			return true;
		}else {
			return this.resultSet.isClosed();
		}
	}
	/**
	 * @return the dataStore
	 */
	public DataSource getDataStore() {
		return dataStore;
	}



	/**
	 * @param dataStore the dataStore to set
	 */
	public void setDataStore(DataSource dataStore) {
		this.dataStore = dataStore;
	}



	/**
	 * @return the connection
	 */
	public Connection getConnection() {
		return connection;
	}



	/**
	 * @param connection the connection to set
	 */
	public void setConnection(Connection connection) {
		this.connection = connection;
	}



	/**
	 * @return the preparedStatement
	 */
	public PreparedStatement getPreparedStatement() {
		return preparedStatement;
	}


	/**
	 * @param preparedStatement the ps to set
	 */
	public void setPreparedStatement(PreparedStatement preparedStatement) {
		this.preparedStatement = preparedStatement;
	}


	/**
	 * @return the initialContext
	 */
	public InitialContext getInitialContext() {
		return initialContext;
	}



	/**
	 * @param initialContext the initialContext to set
	 */
	public void setInitialContext(InitialContext initialContext) {
		this.initialContext = initialContext;
	}



	/**
	 * @return the resultSet
	 */
	public ResultSet getResultSet() {
		return resultSet;
	}

	/**
	 * @param resultSet the resultSet to set
	 */
	public void setResultSet(ResultSet resultSet) {
		this.resultSet = resultSet;
	}
	
	public void commit() throws SQLException {
		this.connection.commit();
	}
	
	public void rollback() throws SQLException {
		this.connection.rollback();
	}
	
	public void setAutoCommitOff() throws SQLException {
		this.connection.setAutoCommit(false);
	}
	
	public void setAutoCommitOn() throws SQLException {
		this.connection.setAutoCommit(true);
	}
	
	public boolean getAutoCommitStatus() throws SQLException{
		return this.connection.getAutoCommit();
	}
	
	public void setString(int index, String value) throws SQLException{
		if(NullPointerExceptionHandler.isNullOrEmpty(value)) {
			this.preparedStatement.setNull(index, java.sql.Types.NULL);
		}else {
			this.preparedStatement.setString(index, value);
		}
	}

}
