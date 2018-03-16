package software.hsharp.business.core

import org.compiere.model.MBPartner
import org.idempiere.common.util.DB
import org.idempiere.common.util.Env
import org.jetbrains.exposed.dao.EntityID
import org.jetbrains.exposed.dao.IntEntity
import org.jetbrains.exposed.dao.IntEntityClass
import org.jetbrains.exposed.dao.IntIdTable
import software.hsharp.business.models.IBusinessPartner
import software.hsharp.business.services.IBusinessPartnerResult
import software.hsharp.business.services.IBusinessPartners
import software.hsharp.business.services.IBusinessPartnersResult
import java.util.*

object c_bpartner : IntIdTable(columnName = "c_bpartner_id") {
    val ad_client_id = integer("ad_client_id")
    val ad_org_id = integer("ad_org_id")
    val isactive = varchar("isactive", 1)
    val created = datetime("created")
    val createdby = integer("createdby")
    val updated = datetime("updated")
    val updatedby = integer("updatedby")
    val c_bpartner_uu= varchar("c_bpartner_uu", 36)

    val name = varchar("name", 60)
    val searchKey = varchar("value", 60)

    val iscustomer = varchar("iscustomer", 1)
}

open class BusinessPartnerModel(id: EntityID<Int>) : IntEntity(id) {
    companion object : IntEntityClass<BusinessPartnerModel>(c_bpartner)

    var c_bpartner_id by c_bpartner.id
    var AD_Client_Id by c_bpartner.ad_client_id
    var AD_Org_Id by c_bpartner.ad_org_id
    var IsActive by c_bpartner.isactive
    var Created by c_bpartner.created
    var CreatedBy by c_bpartner.createdby
    var Updated by c_bpartner.updated
    var UpdatedBy by c_bpartner.updatedby
    var category_Uu by c_bpartner.c_bpartner_uu
    var name by c_bpartner.name
    var searchKey by c_bpartner.searchKey
}

data class BusinessPartner( override val id : Int, override val name : String ) : IBusinessPartner
data class BusinessPartnersResult( override val businessPartners : Array<IBusinessPartner> ) : IBusinessPartnersResult
data class BusinessPartnerResult( override val businessPartner : IBusinessPartner? ) : IBusinessPartnerResult

class BusinessPartners : iDempiereEntities<MBPartner, IBusinessPartner>(), IBusinessPartners {
    override val tableName: String
        get() = "c_bpartner"

    override fun getEntityById(ctx: Properties, id: Int): MBPartner? {
        return MBPartner.get(ctx, id)
    }

    override fun convertToDTO(t: MBPartner): IBusinessPartner {
        return BusinessPartner( t.c_BPartner_ID, t.name )
    }

    override fun getAllBusinessPartners(): IBusinessPartnersResult {
        return BusinessPartnersResult(getAllData().toTypedArray())
    }

    override fun getBusinessPartnerById(id: Int): IBusinessPartnerResult {
        return BusinessPartnerResult(getById(id))
    }
}
