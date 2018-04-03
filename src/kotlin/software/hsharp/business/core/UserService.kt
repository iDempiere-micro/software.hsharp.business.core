package software.hsharp.business.core

import org.compiere.crm.MUser
import org.osgi.service.component.annotations.Component
import software.hsharp.core.models.IUser
import software.hsharp.core.services.IUserServiceImpl
import java.util.*

@Component
class UserService : IUserServiceImpl {
    override fun getUser(ctx: Properties, loginName: String): IUser {
        return MUser.get (ctx, loginName)
    }

}