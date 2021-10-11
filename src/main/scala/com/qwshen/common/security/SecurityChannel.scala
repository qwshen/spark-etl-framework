package com.qwshen.common.security

import org.jasypt.util.text.AES256TextEncryptor

/**
 * Encrypt & decrypt text values
 *
 * @param keyString
 */
final class SecurityChannel(keyString: String) extends Serializable {
  //encryptor
  private val _encryptor = new AES256TextEncryptor()
  //set key
  this._encryptor.setPassword(keyString)

  /**
   * Encrypt the text
   *
   * @param text
   * @return
   */
  def encrypt(text: String): String = this._encryptor.encrypt(text)

  /**
   * Decrypt the text
   *
   * @param text
   * @return
   */
  def decrypt(text: String): String = this._encryptor.decrypt(text)
}
