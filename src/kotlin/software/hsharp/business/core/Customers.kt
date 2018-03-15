package software.hsharp.business.core

import org.compiere.model.MBPartner
import org.idempiere.common.util.DB
import org.idempiere.common.util.Env
import org.jetbrains.exposed.dao.EntityID
import org.jetbrains.exposed.dao.IntEntity
import org.jetbrains.exposed.dao.IntEntityClass
import org.jetbrains.exposed.dao.IntIdTable
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.and
import org.jetbrains.exposed.sql.transactions.transaction
import software.hsharp.business.models.ICategory
import software.hsharp.business.models.ICustomer
import software.hsharp.business.services.ICustomers

object crm_customer_category : IntIdTable(columnName = "customer_category_id") {
    val ad_client_id = integer("ad_client_id")
    val ad_org_id = integer("ad_org_id")
    val isactive = varchar("isactive", 1)
    val created = datetime("created")
    val createdby = integer("createdby")
    val updated = datetime("updated")
    val updatedby = integer("updatedby")
    val customer_category_uu= varchar("customer_category_uu", 36)

    val name = varchar("name", 60)
    val searchKey = varchar("value", 60)

    val customer_id = reference("c_bpartner_id", c_bpartner)
    val category_id = reference("category_id", crm_category)
}

class CustomerCategoryModel(id: EntityID<Int>) : IntEntity(id) {
    companion object : IntEntityClass<CustomerCategoryModel>(crm_customer_category)

    var category_Id by crm_customer_category.id
    var AD_Client_Id by crm_customer_category.ad_client_id
    var AD_Org_Id by crm_customer_category.ad_org_id
    var IsActive by crm_customer_category.isactive
    var Created by crm_customer_category.created
    var CreatedBy by crm_customer_category.createdby
    var Updated by crm_customer_category.updated
    var UpdatedBy by crm_customer_category.updatedby
    var customer_category_uu by crm_customer_category.customer_category_uu
    var name by crm_customer_category.name
    var customer_id by crm_customer_category.customer_id
    var category_id by crm_customer_category.category_id

    val categories by CategoryModel referencedOn crm_customer_category.category_id
    val customers by CustomerModel referencedOn crm_customer_category.customer_id
}

class CustomerModel(id: EntityID<Int>) : BusinessPartnerModel(id) {
    companion object : IntEntityClass<CustomerModel>(c_bpartner)

    val categories by CustomerCategoryModel referrersOn crm_customer_category.customer_id
}

data class Customer( override val id : Int, override val name : String, override val categories : Array<ICategory> ) : ICustomer

class Customers : ICustomers {
    override fun getAllCustomers(): Array<ICustomer> {
        val ctx = Env.getCtx()
        val AD_Org_ID = Env.getAD_Org_ID(ctx)
        val AD_Client_ID = Env.getAD_Client_ID(ctx)
        var result = listOf<ICustomer>()

        Database.connect( { DB.getConnectionRO() } )
        transaction {
            result =
                CustomerModel.find {
                    (c_bpartner.ad_client_id eq AD_Client_ID)
                            .and(c_bpartner.ad_org_id eq AD_Org_ID)
                            .and(c_bpartner.iscustomer eq "Y")
                }.map {
                    Customer( it.id.value, it.name, it.categories.map { Category( it.category_Id.value, it.name ) as ICategory }.toTypedArray() )
                }.filter { MBPartner.get(ctx, it.id) != null }
        }

        return result.toTypedArray()
    }

    override fun getCustomerById(id: Int): ICustomer? {
        val ctx = Env.getCtx()
        val AD_Org_ID = Env.getAD_Org_ID(ctx)
        val AD_Client_ID = Env.getAD_Client_ID(ctx)
        var result : ICustomer? = null

        Database.connect( { DB.getConnectionRO() } )
        transaction {
            result =
                    CustomerModel.find{ (c_bpartner.ad_client_id eq AD_Client_ID)
                            .and( c_bpartner.ad_org_id eq AD_Org_ID )
                            .and( c_bpartner.iscustomer eq "Y" )
                            .and( c_bpartner.id eq id )
                    }
                    .map {
                        Customer( it.id.value, it.name, it.categories.map { Category( it.category_Id.value, it.name ) as ICategory }.toTypedArray() )
                    }.firstOrNull { MBPartner.get(ctx, it.id) != null }
        }
        return result
    }

    override fun getCustomersByAnyCategory(categories: Array<ICategory>): Array<ICustomer> {
        val ctx = Env.getCtx()
        val AD_Org_ID = Env.getAD_Org_ID(ctx)
        val AD_Client_ID = Env.getAD_Client_ID(ctx)
        var result = listOf<ICustomer>()

        Database.connect( { DB.getConnectionRO() } )
        transaction {
            result =
                    CustomerModel.find {
                        (c_bpartner.ad_client_id eq AD_Client_ID)
                                .and(c_bpartner.ad_org_id eq AD_Org_ID)
                                .and(c_bpartner.iscustomer eq "Y")
                    }.map {
                        Customer( it.id.value, it.name, it.categories.map { Category( it.category_Id.value, it.name ) as ICategory }.toTypedArray() )
                    }.filter { MBPartner.get(ctx, it.id) != null && it.categories.intersect(categories.toList()).isNotEmpty() }
        }

        return result.toTypedArray()
    }

}