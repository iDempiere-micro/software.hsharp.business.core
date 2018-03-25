package software.hsharp.business.core

import org.compiere.impl.MBPartner
import org.idempiere.common.util.DB
import org.idempiere.common.util.Env
import org.jetbrains.exposed.dao.EntityID
import org.jetbrains.exposed.dao.IntEntity
import org.jetbrains.exposed.dao.IntEntityClass
import org.jetbrains.exposed.dao.IntIdTable
import org.osgi.service.component.annotations.Component
import software.hsharp.business.core.util.Paging
import software.hsharp.business.models.IBusinessPartner
import software.hsharp.business.services.IBusinessPartnerResult
import software.hsharp.business.services.IBusinessPartnersImpl
import software.hsharp.business.services.IBusinessPartnersResult
import software.hsharp.business.util.DataColumn
import software.hsharp.business.util.DataTable
import software.hsharp.core.models.IDataSource
import software.hsharp.core.models.IPaging
import java.util.*
import software.hsharp.business.models.IBusinessPartnerLocation
import software.hsharp.business.models.ILocation

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

data class Location(
        override val CountryName: String?,
        override val City: String?,
        override val Postal: String?,
        override val Address1: String?,
        override val Address2: String?,
        override val Address3: String?,
        override val Address4: String?,
        override val Address5: String?) : ILocation

data class BusinessPartnerLocation(
        override val Location: ILocation
) : IBusinessPartnerLocation

data class BusinessPartner( 
    override val Key : Int, 
    override val name : String, 
    override val value : String,
    override val Locations: Array<IBusinessPartnerLocation>    
     ) : IBusinessPartner {
    override val ID: String
        get() = ""+Key
}

data class BusinessPartnersResult(
        override val businessPartners : Array<IBusinessPartner>,
        override val __paging: IPaging?) : IBusinessPartnersResult {
    companion object {
        val metadata: IDataSource? get () {
            /*return DataTable(
                    tableName = "c_bpartner",
                    columns = arrayOf(),
                    defaultSortBy = DataColumn(
                            isRequired = true,

                    )
            )*/
            return null
        }
    }

    override val __metadata: IDataSource?
        get() = BusinessPartnersResult.metadata
}

data class BusinessPartnerResult(
        override val businessPartner : IBusinessPartner?,
        override val __paging: IPaging?) : IBusinessPartnerResult {
    override val __metadata: IDataSource?
        get() = BusinessPartnersResult.metadata
}

@Component
class BusinessPartners : iDempiereEntities<MBPartner, IBusinessPartner>(), IBusinessPartnersImpl {
    companion object {
        public fun convertLocations( t: MBPartner ) : Array<IBusinessPartnerLocation> {
            return t.Locations.map {
                BusinessPartnerLocation(
                        Location(
                                CountryName = it.Location.CountryName,
                                City = it.Location.City,
                                Postal = it.Location.Postal,
                                Address1 = it.Location.Address1,
                                Address2 = it.Location.Address2,
                                Address3 = it.Location.Address3,
                                Address4 = it.Location.Address4,
                                Address5 = it.Location.Address5
                        )
                )
            }.toTypedArray()
        }
    }


    override val tableName: String
        get() = "c_bpartner"

    override fun getEntityById(ctx: Properties, id: Int): MBPartner? {
        return MBPartner.get(ctx, id)
    }

    override fun convertToDTO(t: MBPartner): IBusinessPartner {
        val result : BusinessPartner =
                BusinessPartner(
                        t.c_BPartner_ID,
                        t.name,
                        t.value,
                        convertLocations(t)
                )
        return result
    }

    override fun getAllBusinessPartners(): IBusinessPartnersResult {
        return BusinessPartnersResult(getAllData().toTypedArray(), Paging(getCount()))
    }

    override fun getBusinessPartnerById(id: Int): IBusinessPartnerResult {
        val result = getById(id)
        return BusinessPartnerResult(result, if(result==null) {Paging(0)} else {Paging(1)} )
    }
}
