package software.hsharp.business.core

import org.compiere.model.MBPartner
import org.idempiere.common.util.DB
import org.idempiere.common.util.Env
import software.hsharp.business.models.IBusinessPartner
import software.hsharp.business.services.IBusinessPartners
import java.util.*

data class BusinessPartner( override val id : Int, override val name : String ) : IBusinessPartner

class BusinessPartners : iDempiereEntities<MBPartner, IBusinessPartner>(), IBusinessPartners {
    override val tableName: String
        get() = "c_bpartner"

    override fun getEntityById(ctx: Properties, id: Int): MBPartner? {
        return MBPartner.get(ctx, id)
    }

    override fun convertToDTO(t: MBPartner): IBusinessPartner {
        return BusinessPartner( t.c_BPartner_ID, t.name )
    }

    override fun getAllBusinessPartners(): Array<IBusinessPartner> {
        return getAllData().toTypedArray()
    }

    override fun getBusinessPartnerById(id: Int): IBusinessPartner? {
        return getById(id)
    }
}
