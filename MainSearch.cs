using Microsoft.Azure.Search;
using Microsoft.Azure.Search.Models;
using System.Collections.Generic;
using System.Linq;
using trifenix.connect.interfaces.search;
using trifenix.connect.mdm.entity_model;
using trifenix.connect.mdm.enums;
using trifenix.connect.mdm.search.model;

namespace trifenix.connect.search
{

    /// <summary>
    /// Encargada de hacer operaciones CRUD sobre azure search.
    /// Esta clase no debiera ser testeada
    /// </summary>
    /// <typeparam name="GeoPointType">Tipo de dato para geolocalización</typeparam>
    public class MainSearch<GeoPointType> : IBaseEntitySearch<GeoPointType>
    {
        // cliente azure search
        private readonly SearchServiceClient _search;

        


        // opciones cors
        private readonly CorsOptions corsOptions;

        // nombre del servicio en azure
        public string ServiceName { get; private set; }


        // clave del servicio en azure
        public string ServiceKey { get; private set; }


        // índice donde operará el objeto.
        public string Index { get; private set; }


        public MainSearch(string SearchServiceName, string SearchServiceKey, string entityIndex, CorsOptions corsOptions)
        {
            // cliente azure
            _search = new SearchServiceClient(SearchServiceName, new SearchCredentials(SearchServiceKey));
            
            this.corsOptions = corsOptions;
            this.Index = entityIndex;
            this.ServiceName = SearchServiceName;
            this.ServiceKey = SearchServiceKey;

            if (!_search.Indexes.Exists(Index))
                CreateOrUpdateIndex();

        }


        /// <summary>
        /// Añade o elimina items dentro de azure search.
        /// </summary>
        /// <typeparam name="T">El tipo solo puede ser una entidad soportada dentro de azure search, se validará que cumpla</typeparam>
        /// <param name="elements">elementos a guardar dentro del search</param>
        /// <param name="operationType">Tipo de operación Añadir o borrar</param>
        private void OperationElements<T>(List<T> elements, SearchOperation operationType)
        {

            // validar que sea un elemento de tipo search.
            var indexName = Index;

            // obtiene el client azure search de acuerdo al índice.
            var indexClient = _search.Indexes.GetClient(indexName);

            // realiza la acción segun el argumento
            var actions = elements.Select(o => operationType == SearchOperation.Add ? IndexAction.Upload(o) : IndexAction.Delete(o));

            // preparando la ejecución
            var batch = IndexBatch.New(actions);

            // ejecución.
            indexClient.Documents.Index(batch);

            

        }

        /// <summary>
        /// Añade elementos al search.
        /// </summary>
        /// <typeparam name="T">Esto debería ser EntitySearch</typeparam>
        /// <param name="elements"></param>
        public void AddElements(List<IEntitySearch<GeoPointType>> elements)
        {
            OperationElements(elements, SearchOperation.Add);
            
        }

        /// <summary>
        /// Añade un elemento al search.
        /// </summary>
        /// <typeparam name="T">Esto debería ser EntitySearch</typeparam>
        /// <param name="elements"></param>
        public void AddElement(IEntitySearch<GeoPointType> element)
        {
            OperationElements(new List<IEntitySearch<GeoPointType>> { element }, SearchOperation.Add);
            
        }

        /// <summary>
        /// Borra elementos desde el search.
        /// </summary>        
        /// <param name="elements">entidades a eliminar</param>
        public void DeleteElements(List<IEntitySearch<GeoPointType>> elements)
        {
           
            OperationElements(elements, SearchOperation.Delete);
            
        }

        /// <summary>
        /// filtra elementos del search de acuerdo a una conuslta
        /// </summary>
        /// <param name="filter">filtro de azure (Odata)</param>
        /// <returns>Entidades encontradas</returns>
        public List<IEntitySearch<GeoPointType>> FilterElements(string filter)
        {
            var indexName = Index;
            var indexClient = _search.Indexes.GetClient(indexName);
            var result = indexClient.Documents.Search<IEntitySearch<GeoPointType>>(null, new SearchParameters { Filter = filter });

            var filterResult = result.Results.Select(v => (IEntitySearch<GeoPointType>)new EntityBaseSearch<GeoPointType>
            {
                bl = v.Document.bl,
                created = v.Document.created,
                dbl = v.Document.dbl,
                dt = v.Document.dt,
                enm = v.Document.enm,
                geo = v.Document.geo,
                id = v.Document.id,
                index = v.Document.index,
                num32 = v.Document.num32,
                num64 = v.Document.num64,
                rel = v.Document.rel,
                str = v.Document.str,
                sug = v.Document.sug


            }).ToList();

            return filterResult;
        }
        /// <summary>
        /// Vacía el índice.
        /// </summary>
        public void EmptyIndex()
        {
            var indexName = Index;
            _search.Indexes.Delete(indexName);
            CreateOrUpdateIndex();
        }
        /// <summary>
        /// Crea o actualiza el índice en el azure search.
        /// </summary>
        public void CreateOrUpdateIndex()
        {
            var indexName = Index;
            // creación del índice.
            _search.Indexes.CreateOrUpdate(new Index { Name = indexName, Fields = FieldBuilder.BuildForType<EntitySearch>(), CorsOptions = corsOptions, Suggesters = new List<Suggester> { new Suggester { Name="sug", SourceFields=new List<string> { "sug/value" } } } });
        }


        /// <summary>
        /// Borrar elementos de azure search de acuerdo auna consulta
        /// </summary>        
        /// <param name="query">consulta de elementos a eliminar</param>
        public void DeleteElements(string query)
        {
            var elements = FilterElements(query);
            if (elements.Any())
                DeleteElements(elements);
        }

      
    }
}
