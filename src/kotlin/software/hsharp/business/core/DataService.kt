package software.hsharp.business.core

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import org.compiere.orm.DefaultModelFactory
import org.compiere.orm.IModelFactory
import org.compiere.orm.MTable
import org.compiere.process.ProcessInfo
import org.compiere.process.ProcessInfoParameter
import org.compiere.process.ProcessUtil
import org.idempiere.common.util.DB
import org.idempiere.common.util.Env
import org.idempiere.common.util.Env.getAD_Client_ID
import org.idempiere.common.util.Trx
import org.osgi.service.component.annotations.Component
import org.osgi.service.component.annotations.Reference
import software.hsharp.core.models.*
import software.hsharp.core.util.Paging
import software.hsharp.core.utils.parse
import java.io.Serializable
import java.sql.Connection
import java.sql.ResultSet

data class GetDataResult(
        override val rs: ResultSet?,
        override val __metadata: IDataSource?,
        override val __paging: IPaging?) : IGetDataResult {
    companion object {
        val empty : IGetDataResult
            get() = GetDataResult(null, null, null)
    }
}

data class GetRowResult(
        override val rs: ResultSet?,
        override val __metadata: IDataSource?,
        override val __paging: IPaging?) : IGetRowResultData

data class GetRowResultObj(
        override val o: Any?,
        override val __metadata: IDataSource?,
        override val __paging: IPaging?) : IGetRowResultObject

data class UpdateDataResult(
        override val id: Int?,
        override val __metadata: IDataSource?,
        override val __paging: IPaging?) : IUpdateDataResult {
    companion object {
        val empty : IUpdateDataResult
            get() = UpdateDataResult(null, null, null)
    }
}

data class CreateDataResult(
        override val id: Int?,
        override val __metadata: IDataSource?,
        override val __paging: IPaging?) : ICreateDataResult {
    companion object {
        val empty : ICreateDataResult
            get() = CreateDataResult(null, null, null)
    }
}

data class ExecuteJavaProcessResult(
        val message : String?,
        val success : Boolean,
        val result : Serializable?
)

@Component
class DataService : IDataService {
    override fun getTreeData(connection: Connection, root: ITreeDataDescriptor, orderBy: String, orderByOrder: String, offset: Int, limit: Int, filterName1: String, filterValue1: String, filterName2: String, filterValue2: String): IGetTreeDataResult {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun execute(connection: Connection, procName: String, jsonBody: String): String? {
        val ctx = Env.getCtx()
        val ad_Client_ID = Env.getAD_Client_ID(ctx)
        val ad_Org_ID = Env.getAD_Org_ID(ctx)
        val ad_User_ID = Env.getAD_User_ID(ctx)
        val cnn = DB.getConnectionRW()

        set_user(cnn, ad_User_ID)

        var parameters : MutableList<ProcessInfoParameter> = mutableListOf(
                ProcessInfoParameter( "AD_Client_ID", ad_Client_ID.toBigDecimal(), null, null, null ),
                ProcessInfoParameter( "AD_Org_ID", ad_Org_ID.toBigDecimal(), null, null, null )
        )
        val bodyParams = parse(jsonBody)
        parameters
                .addAll( 2, bodyParams.map { ProcessInfoParameter(  it.first, it.second, null, null, null ) } )

        val processInfo = ProcessInfo("Execute Java Process", 0)
        processInfo.aD_Client_ID = ad_Client_ID
        processInfo.aD_User_ID = ad_User_ID
        processInfo.parameter = parameters.toTypedArray()
        processInfo.className = procName
        val m_trx = Trx.get(Trx.createTrxName("ExecuteJavaProcess"), true)
        val success = ProcessUtil.startJavaProcess(ctx, processInfo, m_trx, false)
        val result = ExecuteJavaProcessResult( processInfo.summary, success, processInfo.serializableObject )
        val mapper = ObjectMapper().registerModule(KotlinModule())
        return mapper.writeValueAsString(processInfo.serializableObject)
    }

    override fun getSchemasSupported(connection:Connection) : Array<String> {
        return arrayOf("adempiere", "idempiere")
    }
    override val name: String
        get() = "iDempiere Data Service"

    private fun getType(next: Pair<String, Any>, table: IDataTable): String {
        println( "getting type from $table table for $next" )
        return table.columns.find({ it.columnName.toLowerCase() == next.first.toLowerCase() })!!.columnType
    }

    override fun createData(
            connection: Connection,
            tableName: String,
            table: IDataTable?,
            fields: MutableList<Pair<String, Any>>,
            anonymous_call : Boolean
    ) : ICreateDataResult {
        val ctx = Env.getCtx()
        val ad_Client_ID = Env.getAD_Client_ID(ctx)
        val ad_Org_ID = Env.getAD_Org_ID(ctx)
        val ad_User_ID = Env.getAD_User_ID(ctx)
        val cnn = DB.getConnectionRW()

        set_user(cnn, ad_User_ID)

        val sql =
                ( fields.fold( "INSERT INTO \"${tableName}\" ( \"${tableName}_id\",", { total, next -> total + next.first + "," } ) ) +
                        ( fields.fold(
                                "ad_client_id, ad_org_id, updatedby, updated, createdby, created ) VALUES ( (SELECT COALESCE(MAX(\"${tableName}_id\"),0) + 1 FROM \"${tableName}\" ),",
                                { total, next -> "${total}CAST(? AS ${getType(next, table!!)})," } ) ) + " ?, ?, ?, statement_timestamp(), ?, statement_timestamp()) RETURNING ${tableName}_id;";

        System.out.println( "createData SQL:$sql" );

        val statement = cnn.prepareStatement(sql)
        fields.forEachIndexed { index, value ->
            try {
                statement.setObject(index + 1, value.second)
            } catch ( ex : Exception ) {
                println( "setting $index . parameter to $value failed" )
                throw ex
            }
        }
        val fieldsCount = fields.count()
        statement.setInt(fieldsCount + 1, ad_Client_ID)
        statement.setInt(fieldsCount + 2, ad_Org_ID)
        statement.setInt(fieldsCount + 3, ad_User_ID)
        statement.setInt(fieldsCount + 4, ad_User_ID)

        val rs = statement.executeQuery()
        connection.commit()

        var result : Int? = null
        while (rs.next()) {
            result = rs.getInt(1)
        }

        return CreateDataResult(result, null, null)
    }

    override fun updateData(
            connection: Connection,
            tableName: String,
            id: Int,
            fields: MutableList<Pair<String, Any>>,
            anonymous_call : Boolean
    ) : IUpdateDataResult {
        val ctx = Env.getCtx()
        val ad_Client_ID = Env.getAD_Client_ID(ctx)
        val ad_Org_ID = Env.getAD_Org_ID(ctx)
        val ad_User_ID = Env.getAD_User_ID(ctx)
        val cnn = DB.getConnectionRW()

        set_user(cnn, ad_User_ID)

        val sql =
                ( fields.fold( "UPDATE \"${tableName}\" SET ", { total, next -> total + "${next.first}=?," } ) ) +
                        "ad_client_id = ?, ad_org_id = ? WHERE ${tableName}_id = ? RETURNING ${tableName}_id;";

        System.out.println( "SQL:$sql, ID:$id" );

        val statement = cnn.prepareStatement(sql)
        fields.forEachIndexed { index, value -> statement.setObject( index + 1, value.second ) }

        val fieldsCount = fields.count()
        statement.setInt(fieldsCount + 1, ad_Client_ID)
        statement.setInt(fieldsCount + 2, ad_Org_ID)
        statement.setInt(fieldsCount + 3, id)

        val rs = statement.executeQuery()
        // cnn.commit() <- auto commit

        var result : Int? = null
        while (rs.next()) {
            result = rs.getInt(1)
        }

        return UpdateDataResult(result, null, null)

    }

    private fun set_user(cnn:Connection, ad_User_ID: Int) {
        val identitySelect = "SELECT set_user(?)"
        val identityStatement = cnn.prepareStatement(identitySelect)
        identityStatement.setInt(1, ad_User_ID)
        val identityRs = identityStatement.executeQuery()
        while(identityRs.next()) {}
    }

    override fun getRow(
            connection: Connection,
            tableName: String,
            id: Int,
            anonymous_call : Boolean
    ): IGetRowResult {
        val ctx = Env.getCtx()
        val ad_Client_ID = Env.getAD_Client_ID(ctx)
        val ad_Org_ID = Env.getAD_Org_ID(ctx)
        val ad_User_ID = Env.getAD_User_ID(ctx)
        val cnn = DB.getConnectionRO()

        set_user(cnn, ad_User_ID)

        val tableNameLowerCase = tableName.toLowerCase()
        val selectPart = "SELECT *"
        val sql =  "$selectPart, set_user(?) FROM \"${tableNameLowerCase}\" WHERE (ad_client_id = ? OR ad_client_id=0) AND (ad_org_id = ? OR ad_org_id=0) AND \"${tableNameLowerCase}_id\" = ? "
        println ( "Row SQL:$sql" )
        val statement = cnn.prepareStatement(sql)
        statement.setInt(1, ad_User_ID)
        statement.setInt(2, ad_Client_ID)
        statement.setInt(3, ad_Org_ID)
        statement.setInt(4, id)
        val rs = statement.executeQuery()

        val table = MTable.get( ctx, tableName )
        if ( table == null ) {
            return GetRowResult( rs = rs, __metadata = null, __paging = null )
        } else {
            rs.next()
            val modelFactory : IModelFactory = DefaultModelFactory()
            val result = modelFactory.getPO( tableName, rs, "getRow")
            return GetRowResultObj( o = result, __metadata = null, __paging = null )
        }
    }


    override fun getData(
            connection: Connection, tableName: String,
            columnsRequested : Array<String>?, // null => *
            orderBy : String , // Name
            orderByOrder : String , // ASC | DESC
            offset : Int, // 0
            limit : Int, // 100
            filterName1: String, // Name
            filterValue1: String, // Franta
            filterName2: String, // LastName
            filterValue2: String, // Vokurka
            anonymous_call : Boolean
    ): IGetDataResult {
        val ctx = Env.getCtx()
        val ad_Client_ID = Env.getAD_Client_ID(ctx)
        val ad_Org_ID = Env.getAD_Org_ID(ctx)
        val ad_User_ID = Env.getAD_User_ID(ctx)
        val cnn = DB.getConnectionRO()
        var count = 0

        val where_clause =
                if ( filterName1 != "" ) {
                    " AND \"$filterName1\"=? " +

                            if ( filterName2 != "" ) {
                                " AND \"$filterName2\"=? "
                            } else {
                                ""
                            }

                } else {
                    ""
                }

        val sql_count = "SELECT COUNT(*), set_user(?) FROM \"${tableName}\" WHERE (ad_client_id = ? OR ad_client_id=0) AND (ad_org_id = ? OR ad_org_id=0) $where_clause"
        println ( "SQL (sql_count):$sql_count" )
        val statement_count = cnn.prepareStatement(sql_count)
        statement_count.setInt(1, ad_User_ID)
        statement_count.setInt(2, ad_Client_ID)
        statement_count.setInt(3, ad_Org_ID)
        if ( filterName1 != "" ) {
            statement_count.setString( 3, filterValue1 );
            if ( filterName2 != "" ) { statement_count.setString( 4, filterValue2 )  }
        }

        val rs_count = statement_count.executeQuery()
        while (rs_count.next()) {
            count = rs_count.getInt(1)
        }
        println ( "count:$count" )

        var selectPart =
          if ( columnsRequested == null || columnsRequested!!.count() == 0 )  { "SELECT * " }
          else
            { columnsRequested.fold( "SELECT ", { total, next -> "$total \"$next\"," } ).trimEnd(',') }

        val sql =
                "$selectPart, set_user(?)  FROM \"${tableName}\" WHERE (ad_client_id = ? OR ad_client_id=0) AND (ad_org_id = ? OR ad_org_id=0) $where_clause" +
                if ( orderBy != "" ) {
                    " ORDER BY \"$orderBy\"" + if ( orderByOrder.toLowerCase() == "desc" ) { " desc" } else { "" }
                } else { " ORDER BY 1" } +
                " LIMIT $limit OFFSET $offset;"
        println ( "SQL:$sql" )
        val statement = cnn.prepareStatement(sql)
        statement.setInt(1, ad_User_ID)
        statement.setInt(2, ad_Client_ID)
        statement.setInt(3, ad_Org_ID)
        if ( filterName1 != "" ) {
            statement.setString( 3, filterValue1 );
            if ( filterName2 != "" ) { statement.setString( 4, filterValue2 )  }
        }
        val rs = statement.executeQuery()

        return GetDataResult( rs = rs, __metadata = null, __paging = Paging( count ) )
    }

}

@Component
class DataServiceRegisterHolder {
    companion object {
        var DataServiceRegister: IDataServiceRegister? = null
        var dataService : DataService = DataService()
    }

    @Reference
    fun setDataServiceRegister(dataServiceRegister: IDataServiceRegister) {
        DataServiceRegister = dataServiceRegister
        dataServiceRegister.registerService( dataService )
    }

}