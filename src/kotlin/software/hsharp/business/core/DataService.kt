package software.hsharp.business.core

import org.idempiere.common.util.DB
import org.idempiere.common.util.Env
import org.osgi.service.component.annotations.Component
import org.osgi.service.component.annotations.Reference
import software.hsharp.core.models.*
import software.hsharp.core.util.Paging
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

@Component
class DataService : IDataService {
    override val schemasSupported: Array<String>
        get() = arrayOf("adempiere", "idempiere")
    override val name: String
        get() = "iDempiere Data Service"

    override fun createData(
            connection: Connection,
            tableName: String,
            fields: MutableList<Pair<String, Any>>
    ) : ICreateDataResult {
        TODO( "Implement create data for iDempiere too" )
    }

    override fun updateData(
            connection: Connection,
            tableName: String,
            id: Int,
            fields: MutableList<Pair<String, Any>>) : IUpdateDataResult {
        TODO( "Implement update data for iDempiere too" )
    }

    override fun getData(
            connection: Connection, tableName: String,
            orderBy : String , // Name
            orderByOrder : String , // ASC | DESC
            offset : Int, // 0
            limit : Int, // 100
            filterName1: String, // Name
            filterValue1: String, // Franta
            filterName2: String, // LastName
            filterValue2: String // Vokurka
    ): IGetDataResult {
        val ctx = Env.getCtx()
        val ad_Client_ID = Env.getAD_Client_ID(ctx)
        val ad_Org_ID = Env.getAD_Org_ID(ctx)
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

        val sql_count = "SELECT COUNT(*) FROM \"${tableName}\" WHERE (ad_client_id = ? OR ad_client_id=0) AND (ad_org_id = ? OR ad_org_id=0) $where_clause"
        println ( "SQL (sql_count):$sql_count" )
        val statement_count = cnn.prepareStatement(sql_count)
        statement_count.setInt(1, ad_Client_ID)
        statement_count.setInt(2, ad_Org_ID)
        if ( filterName1 != "" ) {
            statement_count.setString( 3, filterValue1 );
            if ( filterName2 != "" ) { statement_count.setString( 4, filterValue2 )  }
        }

        val rs_count = statement_count.executeQuery()
        while (rs_count.next()) {
            count = rs_count.getInt(1)
        }
        println ( "count:$count" )

        val sql =  "SELECT * FROM \"${tableName}\" WHERE (ad_client_id = ? OR ad_client_id=0) AND (ad_org_id = ? OR ad_org_id=0) $where_clause LIMIT $limit OFFSET $offset;"
        println ( "SQL:$sql" )
        val statement = cnn.prepareStatement(sql)
        statement.setInt(1, ad_Client_ID)
        statement.setInt(2, ad_Org_ID)
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