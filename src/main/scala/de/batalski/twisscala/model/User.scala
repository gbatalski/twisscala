package de.batalski.twisscala.model;
import scala.reflect.BeanProperty

class User(@BeanProperty val username:String, @BeanProperty val password: String) {
    
}
