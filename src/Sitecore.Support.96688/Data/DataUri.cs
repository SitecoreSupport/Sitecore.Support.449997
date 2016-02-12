using System.Runtime.Serialization;
using Sitecore.Data;
using Sitecore.Globalization;

namespace Sitecore.Support.Data
{
  class DataUri : Sitecore.Data.DataUri
  {
    #region Constructors

    public DataUri(Sitecore.Data.DataUri uri) : base(uri)
    {
    }

    public DataUri(ItemUri uri) : base(uri)
    {
    }

    public DataUri(string dataUri) : base(dataUri)
    {
    }

    public DataUri(ID itemID) : base(itemID)
    {
    }

    public DataUri(ID itemID, Language language) : base(itemID, language)
    {
    }

    public DataUri(ID itemID, Language language, Version version) : base(itemID, language, version)
    {
    }

    public DataUri(string path, Language language) : base(path, language)
    {
    }

    public DataUri(string path, Language language, Version version) : base(path, language, version)
    {
    }

    public DataUri(SerializationInfo info, StreamingContext context) : base(info, context)
    {
    }

    #endregion

    public override bool Equals(object obj)
    {
      if (obj == null)
      {
        return false;
      }

      DataUri uri = obj as DataUri;

      if (uri == null)
      {
        return false;
      }

      return this.ItemID == uri.ItemID && this.Version == uri.Version && this.Language == uri.Language && (this.ItemID != (ID)null || Path == uri.Path);
    }

  }
}
