library(DESeq2)

API_ROOT = "https://visualization.genelab.nasa.gov/GLOpenAPI/"
query = paste(sapply(c(
    "study.factor value.spaceflight",
    "investigation.study assays.study assay technology type=RNA Sequencing (RNA-Seq)",
    "study.characteristics.organism=Mus musculus",
    "study.characteristics.material type=Liver",
    "file.datatype=unnormalized counts"
), URLencode), collapse="&")

colData = read.csv(paste0(API_ROOT, "samples/?", query), skip=1)[,c("X.accession", "sample.name", "spaceflight")]
rownames(colData) = make.unique(colData$sample.name)
countData = read.csv(paste0(API_ROOT, "data/?", query), skip=2, check.names=F, row.names=1)
colnames(countData) = make.unique(colnames(countData))
dds = DESeqDataSetFromMatrix(round(na.omit(countData)), colData, ~spaceflight+X.accession)
lrt = DESeq(dds, test="LRT", reduced=~X.accession)
res = results(lrt, contrast=c("spaceflight", "Space.Flight", "Ground.Control"))
write.csv(res[which((res$padj<.05) & (abs(res$log2FoldChange)>2)),], "results.csv")
