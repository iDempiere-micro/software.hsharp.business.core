package software.hsharp.business.core

import org.compiere.model.MBPartner
import org.idempiere.common.util.DB
import org.idempiere.common.util.Env
import software.hsharp.business.models.IBusinessPartner
import software.hsharp.business.services.IBusinessPartners

data class BusinessPartner( override val id : Int, override val name : String ) : IBusinessPartner

class BusinessPartners : IBusinessPartners {
    override fun getAllBusinessPartners(): Array<IBusinessPartner> {
        var dataFromDB : ArrayList<MBPartner> = arrayListOf()
        val connection = DB.getConnectionRO()
        val ctx = Env.getCtx()
        val ad_Client_ID = Env.getAD_Client_ID(ctx)
        val ad_Org_ID = Env.getAD_Org_ID(ctx)
        val queryIdCommand = "SELECT c_bpartner_id FROM adempiere.c_bpartner WHERE (ad_client_id = ? OR ad_client_id=0) AND (ad_org_id = ? OR ad_org_id=0)"
        val stmt = connection.prepareStatement(queryIdCommand)
        stmt.setInt(1, ad_Client_ID)
        stmt.setInt(2, ad_Org_ID)
        val rs = stmt.executeQuery()
        while (rs.next()) {
            val id = rs.getInt(1)
            val data : MBPartner? = MBPartner.get( ctx, id )
            if (data!=null) dataFromDB.add(data)
        }
        rs.close()
        return dataFromDB.map { BusinessPartner( it.c_BPartner_ID, it.name ) }.toTypedArray()
    }

    override fun getBusinessPartnerById(id: Int): IBusinessPartner {
        val ctx = Env.getCtx()
        val data = MBPartner.get( ctx, id )
        return BusinessPartner( data.c_BPartner_ID, data.name )
    }
}
