
using Azure;
using Azure.Search.Documents;
using Azure.Search.Documents.Indexes;
using Azure.Search.Documents.Indexes.Models;
using Azure.Search.Documents.Models;
using Microsoft.Spatial;
using System;
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
    /// <typeparam name="GeographyPoint">Tipo de dato para geolocalización</typeparam>
    public class MainSearch : IBaseEntitySearch<GeographyPoint>
    {



        private readonly SearchIndexClient _searchIndex;
        private readonly SearchClient _search;

        // nombre del servicio en azure
        public string UriService { get; private set; }


        // clave del servicio en azure
        public string ServiceKey { get; private set; }


        // índice donde operará el objeto.
        public string Index { get; private set; }


        /// <summary>
        /// Constructor mainSearch
        /// </summary>
        /// <param name="uriService">endpoint de azure search</param>
        /// <param name="SearchServiceKey">key</param>
        /// <param name="entityIndex">índice del azure search</param>
        public MainSearch(string uriService, string SearchServiceKey, string entityIndex)
        {

            _searchIndex = new SearchIndexClient(new Uri(uriService), new AzureKeyCredential(SearchServiceKey), new SearchClientOptions { 
                
            
            });
            
            

            this.Index = entityIndex;

            this.UriService = uriService;
            this.ServiceKey = SearchServiceKey;

            try
            {   
                _searchIndex.GetIndex(entityIndex);
            }
            catch (RequestFailedException exc)
            {
                if (true)
                {
                    if (exc.Status == 404)
                    {
                        CreateOrUpdateIndex();
                    }
                    else
                    {
                        throw exc;
                    }

                };

            }
            catch (Exception e)
            {
                throw e;
            }




            // cliente azure
            _search = new SearchClient(new Uri(uriService), entityIndex, new AzureKeyCredential(SearchServiceKey));

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

            // realiza la acción segun el argumento
            var actions = elements.Select(o => operationType == SearchOperation.Add ? IndexDocumentsAction.Upload(o) : IndexDocumentsAction.Delete(o)).ToArray();

            // preparando la ejecución
            var batch = IndexDocumentsBatch.Create(actions);

            // ejecución.
            _search.IndexDocuments(batch);
        }

        private EntitySearch Convert(IEntitySearch<GeographyPoint> entity) => new EntitySearch
        {
            id = entity.id,
            index = entity.index,
            bl = entity.bl.Select(s => new BoolProperty { index = s.index, value = s.value }).ToArray(),
            str = entity.str.Select(s => new StrProperty { index = s.index, value = s.value }).ToArray(),
            enm = entity.enm.Select(s => new EnumProperty { index = s.index, value = s.value }).ToArray(),
            dt = entity.dt.Select(s => new DtProperty { index = s.index, value = s.value }).ToArray(),
            dbl = entity.dbl.Select(s => new DblProperty { index = s.index, value = s.value }).ToArray(),
            rel = entity.rel.Select(s => new RelatedId { index = s.index, id = s.id }).ToArray(),
            geo = entity.geo.Select(s => new GeoProperty { index = s.index, value = s.value }).ToArray(),
            sug = entity.sug.Select(s => new StrProperty { index = s.index, value = s.value }).ToArray(),
            num64 = entity.num64.Select(s => new Num64Property { index = s.index, value = s.value }).ToArray(),
            num32 = entity.num32.Select(s => new Num32Property { index = s.index, value = s.value }).ToArray(),
            hh = entity.hh,
            hm = entity.hm,
            created = entity.created,




        };

        /// <summary>
        /// Añade elementos al search.
        /// </summary>
        /// <typeparam name="T">Esto debería ser EntitySearch</typeparam>
        /// <param name="elements"></param>
        public void AddElements(List<IEntitySearch<GeographyPoint>> elements)
        {
            OperationElements(elements.Select(Convert).ToList(), SearchOperation.Add);

        }

        /// <summary>
        /// Añade un elemento al search.
        /// </summary>
        /// <typeparam name="T">Esto debería ser EntitySearch</typeparam>
        /// <param name="elements"></param>
        public void AddElement(IEntitySearch<GeographyPoint> element)
        {
            OperationElements(new List<IEntitySearch<GeographyPoint>> { element }.Select(Convert).ToList(), SearchOperation.Add);

        }

        /// <summary>
        /// Borra elementos desde el search.
        /// </summary>        
        /// <param name="elements">entidades a eliminar</param>
        public void DeleteElements(List<IEntitySearch<GeographyPoint>> elements)
        {

            OperationElements(elements, SearchOperation.Delete);

        }

        /// <summary>
        /// filtra elementos del search de acuerdo a una conuslta
        /// </summary>
        /// <param name="filter">filtro de azure (Odata)</param>
        /// <returns>Entidades encontradas</returns>
        public List<IEntitySearch<GeographyPoint>> FilterElements(string filter)
        {
            var filterOption = new SearchOptions()
            {
                Filter = filter
            };
            var result = _search.Search<EntitySearch>("*", filterOption);
            var filterResult = result.Value.GetResults().Select(v => (IEntitySearch<GeographyPoint>)new EntityBaseSearch<GeographyPoint>
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
                sug = v.Document.sug,
                hh = v.Document.hh,
                hm = v.Document.hm,
            }).ToList();

            return filterResult;
        }
        /// <summary>
        /// Vacía el índice.
        /// </summary>
        public void EmptyIndex()
        {
            var indexName = Index;
            _searchIndex.DeleteIndex(indexName);
            CreateOrUpdateIndex();
        }
        /// <summary>
        /// Crea o actualiza el índice en el azure search.
        /// </summary>
        public void CreateOrUpdateIndex()
        {
            var indexName = Index;
            // creación del índice.
            FieldBuilder fieldBuilder = new FieldBuilder();
            var searchFields = fieldBuilder.Build(typeof(EntitySearch));

            var definition = new SearchIndex(indexName, searchFields);
            var cors_option = new CorsOptions(new List<string> { "*" });
            cors_option.MaxAgeInSeconds = 300;
            definition.CorsOptions = cors_option;
            definition.Suggesters.Add(new SearchSuggester("sug", new List<string> { "sug/value" }));
            //definition.CorsOptions = this.corsOptions;
            _searchIndex.CreateOrUpdateIndex(definition);
            
            

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
