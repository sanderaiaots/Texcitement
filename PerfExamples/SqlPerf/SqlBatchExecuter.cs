using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace SqlPerf {
	public class SqlBatchExecuter {
		public static int ExecuteBatch<T>(SqlCommand cmd, string sql, IEnumerable<T> dataRows, int batchSize) {
			Dictionary<string, int> sqlParams;
			string sqlFormat = ParseSql(sql, out sqlParams);
			Type t = typeof(T);
			FieldInfo[] fields = t.GetFields();
			Dictionary<int, MemberInfo> idxFieldMatch = new Dictionary<int, MemberInfo>();
			for (int i = 0; i < fields.Length; i++) {
				string lowerName = fields[i].Name.ToLower();
				int idx;
				if (sqlParams.TryGetValue(lowerName, out idx)) {
					idxFieldMatch.Add(idx, fields[i]);
				}
			}

			PropertyInfo[] props = t.GetProperties();
			for (int i = 0; i < props.Length; i++) {
				string lowerName = props[i].Name.ToLower();
				int idx;
				if (sqlParams.TryGetValue(lowerName, out idx)) {
					idxFieldMatch.Add(idx, props[i]);
				}
			}
			

			int batchIdx = 0;
			foreach (T data in dataRows) {
				//object[] paramValues = new object[sqlParams.Count];
				string[] newParamNames = new string[sqlParams.Count];
				for (int i = 0; i < newParamNames.Length; i++) {
					MemberInfo xx = idxFieldMatch[i];
					object paramValue;
					if (xx is FieldInfo) {
						paramValue = ((FieldInfo)xx).GetValue(data);
					}
					else {
						paramValue = ((PropertyInfo)xx).GetValue(data);
					}
					newParamNames[i] = "@"+xx.Name+batchIdx;
					cmd.Parameters.AddWithValue(newParamNames[i], paramValue);
				}
				cmd.CommandText += string.Format(sqlFormat, newParamNames)+"\r\n";
        
				if (batchIdx >= batchSize) {
					//TODO: use memory table to catch state of each command
					cmd.ExecuteNonQuery();
					cmd.Parameters.Clear();
					cmd.CommandText = "";
					batchIdx = 0;
				}
				batchIdx++;
			}
			if (cmd.CommandText.Length > 0) {
				cmd.ExecuteNonQuery();
				cmd.Parameters.Clear();
				cmd.CommandText = "";
			}

			return 0;
		}

		//static HashSet<char> 
		public static string ParseSql(string sql, out Dictionary<string, int> paramNames) {
			paramNames = new Dictionary<string, int>();
			StringBuilder positionedStirng = new StringBuilder(sql.Length);
			StringBuilder paramName = new StringBuilder();
			bool paramStrted = false;
			for (int i = 0; i < sql.Length; i++) {
				char c = sql[i];
				if (c == '@') {
					paramStrted = true;
					paramName.Clear();

				}
				else if (paramStrted) {
					if (!char.IsLetterOrDigit(c)) {
						paramStrted = false;
						ParamFound(paramNames, paramName, positionedStirng);
						positionedStirng.Append(c);
					}
					else {
						//use lower case chars in SQL implementation
						paramName.Append(char.ToLower(c));
					}
				}
				else {
					positionedStirng.Append(c);
				}
			}
			if (paramStrted) {
				ParamFound(paramNames, paramName, positionedStirng);
			}
			return positionedStirng.ToString();
		}

		private static void ParamFound(Dictionary<string, int> paramNames, StringBuilder paramName, StringBuilder positionedStirng) {


			string param = paramName.ToString();
			int idx;
			if (!paramNames.TryGetValue(param, out idx)) {
				idx = paramNames.Count;
				paramNames.Add(param, idx);
			}
			positionedStirng.Append('{').Append(idx).Append('}');

		}
	}
	/// <summary>
	/// From: http://www.codeproject.com/Articles/876276/Bulk-Insert-Into-SQL-From-Csharp
	/// </summary>
	/// <typeparam name="TData"></typeparam>
	public class ObjectDataReader<TData> : IDataReader {
		/// <summary>
		/// The enumerator for the IEnumerable{TData} passed to the constructor for 
		/// this instance.
		/// </summary>
		private IEnumerator<TData> dataEnumerator;

		/// <summary>
		/// The lookup of accessor functions for the properties on the TData type.
		/// </summary>
		private Func<TData, object>[] accessors;

		/// <summary>
		/// The lookup of property names against their ordinal positions.
		/// </summary>
		private Dictionary<string, int> ordinalLookup;

		/// <summary>
		/// Initializes a new instance of the <see cref="ObjectDataReader{TData}<TData>"/> class.
		/// </summary>
		/// <param name="data">The data this instance should enumerate through.</param>
		public ObjectDataReader(IEnumerable<TData> data) {
			this.dataEnumerator = data.GetEnumerator();

			// Get all the readable properties for the class and
			// compile an expression capable of reading it
			Type t = typeof(TData);
			var propertyAccessors = t
					.GetProperties(BindingFlags.Instance | BindingFlags.Public)
					.Where(p => p.CanRead)
					.Select((p) => new {
						Property = p.Name,
						Accessor = CreatePropertyAccessor(p)
					})
					.ToList();

			FieldInfo[] fields = t.GetFields();
			propertyAccessors.AddRange(t.GetFields(BindingFlags.Instance | BindingFlags.Public)
					.Select((p) => new {
						Property = p.Name,
						Accessor = CreateFieldAccessor(p)
					}));

			this.accessors = propertyAccessors.Select(p => p.Accessor).ToArray();
			this.ordinalLookup = propertyAccessors.Select((s,i)=>new {s.Property, Index=i}).ToDictionary(
					p => p.Property,
					p => p.Index,
					StringComparer.OrdinalIgnoreCase);
		}

		/// <summary>
		/// Creates a property accessor for the given property information.
		/// </summary>
		/// <param name="p">The property information to generate the accessor for.</param>
		/// <returns>The generated accessor function.</returns>
		private Func<TData, object> CreatePropertyAccessor(PropertyInfo p) {
			// Define the parameter that will be passed - will be the current object
			var parameter = Expression.Parameter(typeof(TData), "input");

			// Define an expression to get the value from the property
			var propertyAccess = Expression.Property(parameter, p.GetGetMethod());

			// Make sure the result of the get method is cast as an object
			var castAsObject = Expression.TypeAs(propertyAccess, typeof(object));

			// Create a lambda expression for the property access and compile it
			var lamda = Expression.Lambda<Func<TData, object>>(castAsObject, parameter);
			return lamda.Compile();
		}

		private Func<TData, object> CreateFieldAccessor(FieldInfo p) {
			// Define the parameter that will be passed - will be the current object
			Func<TData, object> xx = (a)=> p.GetValue(a);
			return xx;
		}

		#region IDataReader Members

		public void Close() {
			this.Dispose();
		}

		public int Depth {
			get { return 1; }
		}

		public DataTable GetSchemaTable() {
			return null;
		}

		public bool IsClosed {
			get { return this.dataEnumerator == null; }
		}

		public bool NextResult() {
			return false;
		}

		public bool Read() {
			if (this.dataEnumerator == null) {
				throw new ObjectDisposedException("ObjectDataReader");
			}
			//Console.WriteLine();
			return this.dataEnumerator.MoveNext();
		}

		public int RecordsAffected {
			get { return -1; }
		}

		#endregion

		#region IDisposable Members

		public void Dispose() {
			this.Dispose(true);
			GC.SuppressFinalize(this);
		}

		protected void Dispose(bool disposing) {
			if (disposing) {
				if (this.dataEnumerator != null) {
					this.dataEnumerator.Dispose();
					this.dataEnumerator = null;
				}
			}
		}

		#endregion

		#region IDataRecord Members

		public int FieldCount {
			get { return this.accessors.Length; }
		}

		public bool GetBoolean(int i) {
			throw new NotImplementedException();
		}

		public byte GetByte(int i) {
			throw new NotImplementedException();
		}

		public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) {
			throw new NotImplementedException();
		}

		public char GetChar(int i) {
			throw new NotImplementedException();
		}

		public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length) {
			throw new NotImplementedException();
		}

		public IDataReader GetData(int i) {
			throw new NotImplementedException();
		}

		public string GetDataTypeName(int i) {
			throw new NotImplementedException();
		}

		public DateTime GetDateTime(int i) {
			throw new NotImplementedException();
		}

		public decimal GetDecimal(int i) {
			throw new NotImplementedException();
		}

		public double GetDouble(int i) {
			throw new NotImplementedException();
		}

		public Type GetFieldType(int i) {
			throw new NotImplementedException();
		}

		public float GetFloat(int i) {
			throw new NotImplementedException();
		}

		public Guid GetGuid(int i) {
			throw new NotImplementedException();
		}

		public short GetInt16(int i) {
			throw new NotImplementedException();
		}

		public int GetInt32(int i) {
			throw new NotImplementedException();
		}

		public long GetInt64(int i) {
			throw new NotImplementedException();
		}

		public string GetName(int i) {
			throw new NotImplementedException();
		}

		public int GetOrdinal(string name) {
			
			int ordinal;
			if (!this.ordinalLookup.TryGetValue(name, out ordinal)) {
				throw new InvalidOperationException("Unknown parameter name " + name);
			}
			//Console.Write("-" + name + "_" + ordinal + "-");
			return ordinal;
		}

		public string GetString(int i) {
			throw new NotImplementedException();
		}

		public object GetValue(int i) {
			if (this.dataEnumerator == null) {
				throw new ObjectDisposedException("ObjectDataReader");
			}

			object v = this.accessors[i](this.dataEnumerator.Current);
			//Console.Write(v + "|");
			return v;
		}

		public int GetValues(object[] values) {
			throw new NotImplementedException();
		}

		public bool IsDBNull(int i) {
			throw new NotImplementedException();
		}

		public object this[string name] {
			get {
				//Console.Write("-"+name+"-");
				int idx = GetOrdinal(name);
				return GetValue(idx);
			}
		}

		public object this[int i] {
			get { return GetValue(i); }
		}

		#endregion
	}
}